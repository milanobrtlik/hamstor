package main

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/milan/hamstor/internal/db"
)

func openIdentityTestDB(t *testing.T) *db.DB {
	t.Helper()
	database, err := db.Open(filepath.Join(t.TempDir(), "hamstor.db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { database.Close() })
	return database
}

// TestS3IdentityNormalization: a trailing slash or a capital letter in a
// hostname is not a different object store, and refusing on one would train
// whoever hit it to reach for --adopt-bucket, which is the one habit that makes
// this check worthless.
func TestS3IdentityNormalization(t *testing.T) {
	same := []struct{ a, b string }{
		{"http://localhost:3900", "http://localhost:3900/"},
		{"http://localhost:3900", "HTTP://LocalHost:3900"},
		{"http://localhost:3900", "  http://localhost:3900  "},
	}
	for _, tt := range same {
		if s3Identity(tt.a, "hamstor") != s3Identity(tt.b, "hamstor") {
			t.Errorf("s3Identity(%q) != s3Identity(%q); these are the same endpoint", tt.a, tt.b)
		}
	}

	// The case this whole check exists for: same bucket name, different store.
	if s3Identity("http://localhost:3900", "hamstor") == s3Identity("https://s3.eu-central-003.backblazeb2.com", "hamstor") {
		t.Fatal("the test Garage and the production B2 backend both use a bucket called \"hamstor\"; " +
			"if their identities collide the check cannot tell them apart")
	}
	if s3Identity("http://localhost:3900", "hamstor") == s3Identity("http://localhost:3900", "hamstor-test") {
		t.Error("different buckets on one endpoint must not share an identity")
	}
}

// TestCheckS3IdentityBindsOnMount covers the ordinary life of the key: mount
// records it, later runs match against it.
func TestCheckS3IdentityBindsOnMount(t *testing.T) {
	database := openIdentityTestDB(t)

	if err := checkS3Identity(database, "http://localhost:3900", "hamstor", true, false); err != nil {
		t.Fatalf("binding on first mount: %v", err)
	}
	stored, err := database.GetConfig(s3IdentityKey)
	if err != nil {
		t.Fatalf("identity was not recorded: %v", err)
	}
	if string(stored) != s3Identity("http://localhost:3900", "hamstor") {
		t.Errorf("stored identity = %q", stored)
	}

	// A subcommand against the same store passes and does not need to record.
	if err := checkS3Identity(database, "http://localhost:3900/", "hamstor", false, false); err != nil {
		t.Errorf("matching identity was refused: %v", err)
	}
}

// TestCheckS3IdentityRefusesADifferentEndpoint is the scenario: the bucket name
// matches, so purge-s3's typed-name prompt would happily accept it, and only the
// endpoint says this is a different store entirely.
func TestCheckS3IdentityRefusesADifferentEndpoint(t *testing.T) {
	database := openIdentityTestDB(t)

	if err := checkS3Identity(database, "https://s3.eu-central-003.backblazeb2.com", "hamstor", true, false); err != nil {
		t.Fatalf("binding: %v", err)
	}

	err := checkS3Identity(database, "http://localhost:3900", "hamstor", false, false)
	if err == nil {
		t.Fatal("a database bound to B2 was allowed to run against a local Garage bucket of the same name")
	}
	if !strings.Contains(err.Error(), "--adopt-bucket") {
		t.Errorf("refusal does not say how to proceed deliberately: %v", err)
	}
}

// TestCheckS3IdentityAdoptRebinds: an endpoint can legitimately change — a
// region alias, http to https — and the way out must not be editing SQLite by
// hand.
func TestCheckS3IdentityAdoptRebinds(t *testing.T) {
	database := openIdentityTestDB(t)

	if err := checkS3Identity(database, "http://old.example.com", "hamstor", true, false); err != nil {
		t.Fatalf("binding: %v", err)
	}
	if err := checkS3Identity(database, "https://new.example.com", "hamstor", false, true); err != nil {
		t.Fatalf("adopt: %v", err)
	}
	// The rebind must stick, or every later run needs the flag again.
	if err := checkS3Identity(database, "https://new.example.com", "hamstor", false, false); err != nil {
		t.Errorf("identity was not rebound: %v", err)
	}
}

// TestCheckS3IdentityUnboundDatabasePasses pins the upgrade path, and the reason
// only mount records. Every database that predates this key arrives unbound; a
// gc that refused them would break on upgrade, and a gc that recorded one would
// enshrine whatever it was mistakenly pointed at as the truth.
func TestCheckS3IdentityUnboundDatabasePasses(t *testing.T) {
	database := openIdentityTestDB(t)

	if err := checkS3Identity(database, "http://localhost:3900", "hamstor", false, false); err != nil {
		t.Fatalf("an unbound database was refused: %v", err)
	}
	if _, err := database.GetConfig(s3IdentityKey); err == nil {
		t.Error("a non-mount run recorded an identity; only the daemon may bind a database")
	}
}
