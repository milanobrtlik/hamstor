package s3store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	maxRetries    = 3
	retryBaseWait = 500 * time.Millisecond

	// MaxDownloadSize bounds one whole-object download. It used to be 2 GB and
	// it used to be a ceiling on FILE size — an inode named one object, so a
	// larger file could not be read back at all, and could not be opened for
	// writing even to append a line.
	//
	// Under the block layout no object can reach it: a file is N blocks of at
	// most db.BlockSize (8 MiB) plus 29 bytes of GCM overhead, and a volume is
	// sealed at TargetVolumeSize (8 MB) plus at most one MaxNeedleSize needle.
	// Both whole-object callers (fetchBlock, fetchVolume) therefore ask for far
	// less than this, so it is now what it claims to be: a sanity check against a
	// corrupt or hostile object, not a limit anyone can hit legitimately.
	//
	// Tied to UploadPartSize because that is the real structural bound — nothing
	// hamstor writes is ever larger than one part (D1).
	MaxDownloadSize = UploadPartSize

	// UploadPartSize is the transfer manager's part size. It is deliberately
	// twice the 8 MiB block size of the block layout, so that a single block is
	// always one PutObject and no path in hamstor can produce a multipart
	// object. See claudedocs/block-layout-design.md, D1.
	//
	// That is a structural property, not a heuristic: the SDK does one
	// nextReader() and takes the single-part path when it returns io.EOF, which
	// it does exactly when the remaining bytes fit in one part
	// (manager/upload.go:391, :472). So size <= PartSize implies one PUT.
	//
	// Raising it costs no memory as long as every request body implements
	// io.ReaderAt — see ReaderAtSeeker.
	UploadPartSize = 16 << 20
)

// ReaderAtSeeker is what the SDK's transfer manager needs from a request body to
// stream it without buffering. It mirrors the manager's own readerAtSeeker
// (manager/upload.go:908), which is unexported there.
//
// This is a requirement, not a coincidence. For a body of this shape nextReader
// hands out an io.SectionReader and allocates nothing; for anything less it
// falls into the branch that fills a buffer from the pool
// (manager/upload.go:503), which means Concurrency+1 = 6 slices of
// UploadPartSize per upload. With UploadSem allowing 32 concurrent uploads
// against debug.SetMemoryLimit(150 << 20), that kills the mount immediately.
// Hence the parameter type on UploadReader: the compiler holds the invariant.
type ReaderAtSeeker interface {
	io.ReadSeeker
	io.ReaderAt
}

type Store struct {
	client   *s3.Client
	uploader *manager.Uploader
	bucket   string
}

func New(ctx context.Context, bucket, endpoint, accessKey, secretKey, region string) (*Store, error) {
	var cfgOpts []func(*config.LoadOptions) error
	if region != "" {
		cfgOpts = append(cfgOpts, config.WithRegion(region))
	}
	if accessKey != "" {
		cfgOpts = append(cfgOpts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
		))
	}

	// Do not validate response checksums unless a request asks for it.
	//
	// The SDK default is ResponseChecksumValidationWhenSupported: validate
	// whenever the server reports a checksum. That is unusable for us, because
	// for an object uploaded via multipart the reported CRC32 is the checksum of
	// the concatenated PART checksums, not of the object's bytes — so a
	// whole-object GET can never validate, and Download fails all three retries
	// with "checksum did not match: algorithm CRC32". Measured 2026-07-21: a 9 MB
	// object (multipart at the default 5 MiB part size) is undownloadable against
	// Garage, while a range GET of the same object returns the correct bytes. The
	// data is fine; only the whole-object validation is impossible.
	//
	// Whether it bites depends on how the backend reports the checksum, which is
	// why this looked backend-specific for a while: B2 (production) appends the
	// part count, the SDK recognises the composite and logs "Skipped validation of
	// multipart checksum"; Garage reports a bare value and the SDK validates it.
	// Relying on that difference is not a strategy — it makes the mount work or
	// not depending on the S3 implementation behind it.
	//
	// What this gives up: no CRC check on GET for single-PUT objects either.
	// Under encryption nothing is lost (AES-256-GCM is authenticated — a corrupted
	// ciphertext fails to open); unencrypted mounts fall back to TLS plus the
	// provider's own integrity. That is the accepted trade, and it is a far better
	// one than large files being unreadable.
	//
	// A LoadOption wins over AWS_RESPONSE_CHECKSUM_VALIDATION in the environment
	// (config resolution is first-match-wins with LoadOptions first), so this
	// cannot be silently undone from outside.
	cfgOpts = append(cfgOpts, config.WithResponseChecksumValidation(
		aws.ResponseChecksumValidationWhenRequired,
	))

	cfg, err := config.LoadDefaultConfig(ctx, cfgOpts...)
	if err != nil {
		return nil, fmt.Errorf("s3store: load config: %w", err)
	}

	var opts []func(*s3.Options)
	if endpoint != "" {
		opts = append(opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		})
	}

	client := s3.NewFromConfig(cfg, opts...)
	return &Store{
		client: client,
		uploader: manager.NewUploader(client, func(u *manager.Uploader) {
			u.PartSize = UploadPartSize
		}),
		bucket: bucket,
	}, nil
}

// retry executes fn up to maxRetries times with exponential backoff.
// Returns immediately on context cancellation or non-retryable errors.
func retry(ctx context.Context, op string, fn func() error) error {
	var lastErr error
	for attempt := range maxRetries {
		lastErr = fn()
		if lastErr == nil {
			return nil
		}
		if ctx.Err() != nil {
			return lastErr
		}
		// Don't retry NoSuchKey - it's permanent
		var nsk *types.NoSuchKey
		if errors.As(lastErr, &nsk) {
			return lastErr
		}
		if attempt < maxRetries-1 {
			wait := retryBaseWait * time.Duration(1<<attempt)
			log.Printf("s3store: %s attempt %d failed, retrying in %v: %v", op, attempt+1, wait, lastErr)
			select {
			case <-time.After(wait):
			case <-ctx.Done():
				return lastErr
			}
		}
	}
	return lastErr
}

func (s *Store) Upload(ctx context.Context, key string, data []byte) error {
	return retry(ctx, "upload "+key, func() error {
		_, err := s.uploader.Upload(ctx, &s3.PutObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(data),
		})
		if err != nil {
			return fmt.Errorf("s3store: upload %s: %w", key, err)
		}
		return nil
	})
}

// UploadReader uploads data from a reader. The S3 upload manager handles
// multipart uploads automatically for large files — though with UploadPartSize
// set as it is, nothing hamstor writes today should reach that path.
//
// The parameter is a ReaderAtSeeker rather than an io.ReadSeeker on purpose: it
// is what keeps the body out of the SDK's buffering branch. See ReaderAtSeeker.
func (s *Store) UploadReader(ctx context.Context, key string, r ReaderAtSeeker, size int64) error {
	return retry(ctx, "upload "+key, func() error {
		if _, err := r.Seek(0, io.SeekStart); err != nil {
			return fmt.Errorf("s3store: seek before upload %s: %w", key, err)
		}
		_, err := s.uploader.Upload(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(s.bucket),
			Key:           aws.String(key),
			Body:          r,
			ContentLength: aws.Int64(size),
		})
		if err != nil {
			return fmt.Errorf("s3store: upload %s: %w", key, err)
		}
		return nil
	})
}

func (s *Store) Download(ctx context.Context, key string) ([]byte, error) {
	var result []byte
	err := retry(ctx, "download "+key, func() error {
		out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			return fmt.Errorf("s3store: download %s: %w", key, err)
		}
		defer out.Body.Close()
		limited := io.LimitReader(out.Body, MaxDownloadSize+1)
		data, err := io.ReadAll(limited)
		if err != nil {
			return fmt.Errorf("s3store: read body %s: %w", key, err)
		}
		if int64(len(data)) > MaxDownloadSize {
			return fmt.Errorf("s3store: download %s: object exceeds %d byte limit", key, MaxDownloadSize)
		}
		result = data
		return nil
	})
	return result, err
}

// DownloadRange downloads a byte range from S3 using the Range header.
// It requires the response to contain exactly `length` bytes: a short read
// (range partially past EOF, a truncating proxy/CDN, or an object shorter than
// the DB-recorded needle bounds) is treated as a retryable error rather than
// silently serving or persisting truncated data.
func (s *Store) DownloadRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	if length <= 0 {
		return nil, nil
	}
	rangeStr := fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
	var result []byte
	err := retry(ctx, "download-range "+key, func() error {
		out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(key),
			Range:  aws.String(rangeStr),
		})
		if err != nil {
			return fmt.Errorf("s3store: download range %s [%s]: %w", key, rangeStr, err)
		}
		defer out.Body.Close()
		data, err := io.ReadAll(out.Body)
		if err != nil {
			return fmt.Errorf("s3store: read range body %s: %w", key, err)
		}
		if int64(len(data)) != length {
			return fmt.Errorf("s3store: short range read %s [%s]: got %d want %d", key, rangeStr, len(data), length)
		}
		result = data
		return nil
	})
	return result, err
}

func (s *Store) List(ctx context.Context, prefix string) ([]string, error) {
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	})
	var keys []string
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("s3store: list prefix %q: %w", prefix, err)
		}
		for _, obj := range page.Contents {
			keys = append(keys, *obj.Key)
		}
	}
	return keys, nil
}

// S3Object holds key and metadata from a listing.
type S3Object struct {
	Key          string
	LastModified time.Time
}

// ListObjects returns keys with timestamps. Used by GC for grace period filtering.
func (s *Store) ListObjects(ctx context.Context, prefix string) ([]S3Object, error) {
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	})
	var objects []S3Object
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("s3store: list prefix %q: %w", prefix, err)
		}
		for _, obj := range page.Contents {
			var lastMod time.Time
			if obj.LastModified != nil {
				lastMod = *obj.LastModified
			}
			objects = append(objects, S3Object{Key: *obj.Key, LastModified: lastMod})
		}
	}
	return objects, nil
}

func (s *Store) Copy(ctx context.Context, srcKey, dstKey string) error {
	return retry(ctx, "copy "+srcKey, func() error {
		_, err := s.client.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:     aws.String(s.bucket),
			CopySource: aws.String(s.bucket + "/" + srcKey),
			Key:        aws.String(dstKey),
		})
		if err != nil {
			return fmt.Errorf("s3store: copy %s -> %s: %w", srcKey, dstKey, err)
		}
		return nil
	})
}

// DeleteBatch deletes up to len(keys) objects using S3 multi-object delete.
// Returns the number of successfully deleted objects.
func (s *Store) DeleteBatch(ctx context.Context, keys []string) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	const batchSize = 1000
	deleted := 0
	for i := 0; i < len(keys); i += batchSize {
		end := i + batchSize
		if end > len(keys) {
			end = len(keys)
		}
		batch := keys[i:end]
		objects := make([]types.ObjectIdentifier, len(batch))
		for j, key := range batch {
			objects[j] = types.ObjectIdentifier{Key: aws.String(key)}
		}
		var batchDeleted int
		err := retry(ctx, fmt.Sprintf("delete-batch (%d objects)", len(batch)), func() error {
			out, err := s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
				Bucket: aws.String(s.bucket),
				Delete: &types.Delete{
					Objects: objects,
					Quiet:   aws.Bool(true),
				},
			})
			if err != nil {
				return fmt.Errorf("s3store: delete batch: %w", err)
			}
			if len(out.Errors) > 0 {
				return fmt.Errorf("s3store: delete batch: %d errors, first: %s: %s",
					len(out.Errors), aws.ToString(out.Errors[0].Key), aws.ToString(out.Errors[0].Message))
			}
			batchDeleted = len(batch)
			return nil
		})
		if err != nil {
			return deleted, err
		}
		deleted += batchDeleted
	}
	return deleted, nil
}

func (s *Store) Delete(ctx context.Context, key string) error {
	return retry(ctx, "delete "+key, func() error {
		_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			var nsk *types.NoSuchKey
			if errors.As(err, &nsk) {
				return nil
			}
			return fmt.Errorf("s3store: delete %s: %w", key, err)
		}
		return nil
	})
}
