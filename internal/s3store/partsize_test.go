package s3store

// Internal test package: it reads Store.uploader and builds a Store around a
// stub client, neither of which is reachable from outside.
//
// These tests need no S3. manager.UploadAPIClient is an interface, so the
// transfer manager can be driven against a stub that answers instantly and
// never reads the body — which is exactly right here, because what is under
// test is which branch the manager takes, not what crosses the wire.

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// stubUploadClient satisfies manager.UploadAPIClient and does nothing. It never
// touches the request body, so any allocation observed around an Upload call is
// the manager's own.
type stubUploadClient struct{}

func (stubUploadClient) PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return &s3.PutObjectOutput{}, nil
}

func (stubUploadClient) UploadPart(context.Context, *s3.UploadPartInput, ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	return &s3.UploadPartOutput{ETag: aws.String("\"stub\"")}, nil
}

func (stubUploadClient) CreateMultipartUpload(context.Context, *s3.CreateMultipartUploadInput, ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	return &s3.CreateMultipartUploadOutput{UploadId: aws.String("stub-upload-id")}, nil
}

func (stubUploadClient) CompleteMultipartUpload(context.Context, *s3.CompleteMultipartUploadInput, ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	return &s3.CompleteMultipartUploadOutput{}, nil
}

func (stubUploadClient) AbortMultipartUpload(context.Context, *s3.AbortMultipartUploadInput, ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	return &s3.AbortMultipartUploadOutput{}, nil
}

// stubStore is a Store wired to the stub client, with the same part size the
// production constructor configures.
func stubStore() *Store {
	return &Store{
		uploader: manager.NewUploader(stubUploadClient{}, func(u *manager.Uploader) {
			u.PartSize = UploadPartSize
		}),
		bucket: "test-bucket",
	}
}

// seekerOnly hides the ReaderAt that *bytes.Reader has, leaving a plain
// io.ReadSeeker. It exists only as the control case below.
type seekerOnly struct{ r *bytes.Reader }

func (s *seekerOnly) Read(p []byte) (int, error) { return s.r.Read(p) }

func (s *seekerOnly) Seek(off int64, whence int) (int64, error) { return s.r.Seek(off, whence) }

// allocatedDuring reports the bytes allocated while fn ran. TotalAlloc is
// cumulative and monotonic, so GC cannot skew it, and it counts every goroutine
// — which matters because the manager uploads parts concurrently. Upload
// returns only after every part is done, so the second reading is a valid
// barrier.
func allocatedDuring(fn func()) uint64 {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	fn()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// TestUploaderPartSize pins the part size the production constructor
// configures. The block layout depends on it: a block must never be big enough
// to become a multipart object.
func TestUploaderPartSize(t *testing.T) {
	// Offline: an explicit region plus static credentials means
	// LoadDefaultConfig resolves nothing over the network, and no request is
	// ever made, so the unroutable endpoint is never dialled.
	store, err := New(context.Background(), "bucket", "http://127.0.0.1:1", "ak", "sk", "us-east-1")
	if err != nil {
		t.Fatalf("create store: %v", err)
	}

	if store.uploader.PartSize != UploadPartSize {
		t.Errorf("uploader part size = %d, want %d (see UploadPartSize)",
			store.uploader.PartSize, UploadPartSize)
	}
	if UploadPartSize < manager.MinUploadPartSize {
		t.Errorf("UploadPartSize %d is below the SDK minimum %d; Upload would fail outright",
			UploadPartSize, manager.MinUploadPartSize)
	}
	// The point of the constant: one 8 MiB block is one PUT, with room for the
	// 29 bytes of AES-GCM overhead and then some.
	if UploadPartSize < 2*(8<<20) {
		t.Errorf("UploadPartSize %d is under twice the 8 MiB block size; blocks could go multipart",
			UploadPartSize)
	}
}

// TestUploadBodiesAvoidPartBuffers is the sanity test for the memory half of D1.
//
// Raising PartSize to 16 MiB is free only while every request body implements
// io.ReaderAt: the manager then slices it with io.NewSectionReader and allocates
// nothing. A body without ReaderAt falls into the branch that fills a pooled
// buffer of PartSize per part, and with UploadSem at 32 concurrent uploads that
// overruns debug.SetMemoryLimit(150 << 20) at once.
//
// The control case at the end is what makes the rest meaningful — it shows the
// measurement can see a part buffer when one is allocated. If only the control
// fails, the SDK stopped taking the zero-copy branch and D1's premise is gone.
func TestUploadBodiesAvoidPartBuffers(t *testing.T) {
	store := stubStore()
	ctx := context.Background()

	// Comfortably more than one part, so the multipart path runs and nextReader
	// is called several times.
	size := int64(UploadPartSize * 5 / 2)
	data := make([]byte, size)

	// A part buffer would be UploadPartSize; anything the manager legitimately
	// allocates is orders of magnitude smaller.
	const budget = UploadPartSize / 4

	t.Run("Upload", func(t *testing.T) {
		var err error
		got := allocatedDuring(func() {
			err = store.Upload(ctx, "key-bytes", data)
		})
		if err != nil {
			t.Fatalf("upload: %v", err)
		}
		if got >= budget {
			t.Errorf("Upload of %d bytes allocated %d, want under %d — "+
				"the request body is no longer io.ReaderAt, so the manager is buffering %d-byte parts",
				size, got, budget, UploadPartSize)
		}
	})

	t.Run("UploadReader", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "spill")
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatalf("write spill file: %v", err)
		}
		f, err := os.Open(path)
		if err != nil {
			t.Fatalf("open spill file: %v", err)
		}
		defer f.Close()

		var upErr error
		got := allocatedDuring(func() {
			upErr = store.UploadReader(ctx, "key-file", f, size)
		})
		if upErr != nil {
			t.Fatalf("upload reader: %v", upErr)
		}
		if got >= budget {
			t.Errorf("UploadReader of %d bytes allocated %d, want under %d — "+
				"the spill file is streamed, so nothing of this size should be buffered",
				size, got, budget)
		}
	})

	t.Run("control/not a ReaderAt", func(t *testing.T) {
		// Bypasses Store.UploadReader deliberately: its signature no longer
		// admits a body like this, which is the point. This drives the manager
		// directly to show what that signature is preventing.
		body := &seekerOnly{r: bytes.NewReader(data)}
		var err error
		got := allocatedDuring(func() {
			_, err = store.uploader.Upload(ctx, &s3.PutObjectInput{
				Bucket: aws.String(store.bucket),
				Key:    aws.String("key-control"),
				Body:   body,
			})
		})
		if err != nil {
			t.Fatalf("control upload: %v", err)
		}
		if got < UploadPartSize {
			t.Errorf("a body that is not io.ReaderAt allocated only %d bytes, expected at least one "+
				"%d-byte part buffer — the measurement is not seeing part buffers, so the "+
				"assertions above prove nothing", got, UploadPartSize)
		}
	})
}

// Compile-time record of the two body types the package actually constructs.
// The signatures already enforce this; these say out loud which concrete types
// are relied on, so a change to either call site reads as the invariant it is.
var (
	_ ReaderAtSeeker = (*bytes.Reader)(nil) // Upload
	_ ReaderAtSeeker = (*os.File)(nil)      // UploadReader: spill file, retained pending file
	_ io.ReadSeeker  = ReaderAtSeeker(nil)  // still usable everywhere an io.ReadSeeker was
)
