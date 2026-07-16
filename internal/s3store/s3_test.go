package s3store_test

import (
	"context"
	"testing"

	"github.com/milan/hamstor/internal/s3store"
	"github.com/milan/hamstor/internal/testutil"
)

func setupStore(t *testing.T) *s3store.Store {
	t.Helper()
	cfg := testutil.RequireS3(t)
	store, err := s3store.New(context.Background(), cfg.Bucket, cfg.Endpoint, cfg.AccessKey, cfg.SecretKey, cfg.Region)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	return store
}

// TestDownloadRangeExact verifies the happy path: a fully satisfiable range
// returns exactly the requested bytes.
func TestDownloadRangeExact(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()

	key := s3store.NewKey()
	content := []byte("0123456789")
	if err := store.Upload(ctx, key, content); err != nil {
		t.Fatalf("upload: %v", err)
	}
	t.Cleanup(func() { store.Delete(context.Background(), key) })

	got, err := store.DownloadRange(ctx, key, 2, 5)
	if err != nil {
		t.Fatalf("download range: %v", err)
	}
	if string(got) != "23456" {
		t.Errorf("range [2,5) = %q, want %q", got, "23456")
	}
}

// TestDownloadRangeShortReadIsError is the regression test for silent data
// truncation: asking for more bytes than the object holds makes S3 clamp the
// range to what exists. Returning those bytes as if they were the full range
// would serve (and cache) truncated data as valid. The short read must surface
// as an error instead.
func TestDownloadRangeShortReadIsError(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()

	key := s3store.NewKey()
	content := []byte("0123456789") // 10 bytes
	if err := store.Upload(ctx, key, content); err != nil {
		t.Fatalf("upload: %v", err)
	}
	t.Cleanup(func() { store.Delete(context.Background(), key) })

	got, err := store.DownloadRange(ctx, key, 0, 100) // ask for 100, only 10 exist
	if err == nil {
		t.Fatalf("short range read returned no error (got %d bytes: %q)", len(got), got)
	}
	if got != nil {
		t.Errorf("short range read returned data alongside error: %q", got)
	}
}

// TestDownloadRangeZeroLength verifies a non-positive length short-circuits
// without issuing a request (a bytes=0--1 header would be malformed).
func TestDownloadRangeZeroLength(t *testing.T) {
	store := setupStore(t)

	got, err := store.DownloadRange(context.Background(), "nonexistent-key", 0, 0)
	if err != nil {
		t.Errorf("zero-length range returned error: %v", err)
	}
	if got != nil {
		t.Errorf("zero-length range returned %d bytes, want nil", len(got))
	}
}
