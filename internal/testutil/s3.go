// Package testutil provides shared helpers for tests that need a live
// S3-compatible endpoint (see .env.test).
package testutil

import (
	"net"
	"net/url"
	"os"
	"testing"
	"time"
)

// S3Config holds the S3 connection settings used by tests.
type S3Config struct {
	Endpoint  string
	Bucket    string
	AccessKey string
	SecretKey string
	Region    string
}

// Env returns the first non-empty value of the test-specific key, then the
// production fallback key, then defaultVal.
func Env(testKey, fallbackKey, defaultVal string) string {
	if v := os.Getenv(testKey); v != "" {
		return v
	}
	if v := os.Getenv(fallbackKey); v != "" {
		return v
	}
	return defaultVal
}

// RequireS3 returns the configured S3 settings, or skips the test if no
// credentials are set or the endpoint does not accept a connection.
//
// The probe matters because the store constructor never dials: without it an
// unconfigured checkout does not skip but instead burns ~30s per test on SDK
// retries (falling back to EC2 IMDS when credentials are empty) before failing,
// which is indistinguishable from a real regression.
func RequireS3(t *testing.T) S3Config {
	t.Helper()

	cfg := S3Config{
		Endpoint:  Env("HAMSTOR_TEST_ENDPOINT", "HAMSTOR_ENDPOINT", "http://localhost:3900"),
		Bucket:    Env("HAMSTOR_TEST_BUCKET", "HAMSTOR_BUCKET", "hamstor"),
		AccessKey: Env("HAMSTOR_TEST_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID", ""),
		SecretKey: Env("HAMSTOR_TEST_SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY", ""),
		Region:    Env("HAMSTOR_TEST_REGION", "AWS_REGION", ""),
	}

	if cfg.AccessKey == "" || cfg.SecretKey == "" {
		t.Skip("no S3 credentials configured; run `source .env.test` to enable S3 tests")
	}

	u, err := url.Parse(cfg.Endpoint)
	if err != nil || u.Host == "" {
		t.Skipf("unusable S3 endpoint %q: %v", cfg.Endpoint, err)
	}
	host := u.Host
	if u.Port() == "" {
		port := "80"
		if u.Scheme == "https" {
			port = "443"
		}
		host = net.JoinHostPort(host, port)
	}
	conn, err := net.DialTimeout("tcp", host, 2*time.Second)
	if err != nil {
		t.Skipf("S3 endpoint %s unreachable: %v", cfg.Endpoint, err)
	}
	conn.Close()

	return cfg
}
