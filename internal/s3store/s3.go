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
)

type Store struct {
	client *s3.Client
	bucket string
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

	return &Store{
		client: s3.NewFromConfig(cfg, opts...),
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
		uploader := manager.NewUploader(s.client)
		_, err := uploader.Upload(ctx, &s3.PutObjectInput{
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
// multipart uploads automatically for large files.
func (s *Store) UploadReader(ctx context.Context, key string, r io.ReadSeeker, size int64) error {
	return retry(ctx, "upload "+key, func() error {
		r.Seek(0, io.SeekStart)
		uploader := manager.NewUploader(s.client)
		_, err := uploader.Upload(ctx, &s3.PutObjectInput{
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
		data, err := io.ReadAll(out.Body)
		if err != nil {
			return fmt.Errorf("s3store: read body %s: %w", key, err)
		}
		result = data
		return nil
	})
	return result, err
}

// DownloadRange downloads a byte range from S3 using the Range header.
func (s *Store) DownloadRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
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
