package s3store

// Internal test package: it needs to replace the Store's uploader to force a
// multipart upload regardless of whatever part size New happens to configure.

import (
	"bytes"
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/milan/hamstor/internal/testutil"
)

// forceMultipartStore returns a Store built by the production constructor (so
// the real client config, including response checksum validation, is under
// test) with an uploader pinned to the smallest legal part size.
//
// Pinning it matters: the payload below is multipart only relative to the part
// size, so a later change that raises PartSize would otherwise turn this test
// into a single-PUT test that passes without exercising anything.
func forceMultipartStore(t *testing.T) (*Store, int64) {
	t.Helper()
	cfg := testutil.RequireS3(t)

	store, err := New(context.Background(), cfg.Bucket, cfg.Endpoint, cfg.AccessKey, cfg.SecretKey, cfg.Region)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}

	partSize := manager.MinUploadPartSize // 5 MiB, the smallest the SDK accepts
	store.uploader = manager.NewUploader(store.client, func(u *manager.Uploader) {
		u.PartSize = partSize
	})
	return store, partSize
}

// TestDownloadWholeMultipartObject is the regression test for the CRC32 trap.
//
// With the SDK default (ResponseChecksumValidationWhenSupported) this fails on
// any backend that reports a bare checksum for a multipart object: the value
// covers the concatenated part checksums, not the bytes, so validation cannot
// pass and Download burns all three retries before returning an error. That made
// large files unreadable for writing (Open's write preload calls Download) and
// unreadable outright when encrypted (encryption disables the range path).
//
// Nothing about the stored bytes is wrong, which is what makes it worth a test:
// the failure looks like corruption and is not.
func TestDownloadWholeMultipartObject(t *testing.T) {
	store, partSize := forceMultipartStore(t)
	ctx := context.Background()

	// Comfortably over one part, so the upload is genuinely multipart.
	data := make([]byte, partSize+(2<<20))
	for i := range data {
		data[i] = byte(i * 7)
	}

	key := NewKey()
	if err := store.Upload(ctx, key, data); err != nil {
		t.Fatalf("upload %d bytes: %v", len(data), err)
	}
	t.Cleanup(func() { store.Delete(context.Background(), key) })

	got, err := store.Download(ctx, key)
	if err != nil {
		t.Fatalf("whole-object download of a multipart object failed: %v\n"+
			"(if this is a checksum mismatch, response checksum validation is back on "+
			"— see the comment in New)", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("downloaded %d bytes, want %d, and contents differ", len(got), len(data))
	}
}

// TestDownloadRangeOfMultipartObject pins the other half of the diagnosis: range
// GETs of the same object were always fine. If this ever fails alongside the test
// above, the problem is the object, not the checksum configuration.
func TestDownloadRangeOfMultipartObject(t *testing.T) {
	store, partSize := forceMultipartStore(t)
	ctx := context.Background()

	data := make([]byte, partSize+(2<<20))
	for i := range data {
		data[i] = byte(i * 7)
	}

	key := NewKey()
	if err := store.Upload(ctx, key, data); err != nil {
		t.Fatalf("upload: %v", err)
	}
	t.Cleanup(func() { store.Delete(context.Background(), key) })

	// Straddle the part boundary, where a composite-checksum bug would show up.
	off := partSize - 512
	got, err := store.DownloadRange(ctx, key, off, 1024)
	if err != nil {
		t.Fatalf("range read across the part boundary failed: %v", err)
	}
	if !bytes.Equal(got, data[off:off+1024]) {
		t.Fatal("range read across the part boundary returned wrong bytes")
	}
}
