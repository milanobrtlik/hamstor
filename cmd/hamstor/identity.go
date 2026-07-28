package main

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/milan/hamstor/internal/db"
)

// s3IdentityKey is the config row binding a database to the object store it
// describes.
const s3IdentityKey = "s3_identity"

// s3Identity normalizes the pair a database is bound to.
//
// The bucket name alone is not enough. The local Garage used for tests and the
// production Backblaze backend both hold a bucket called "hamstor", so only the
// endpoint tells them apart — and a mistaken --endpoint is exactly the accident
// purge-s3's typed-name prompt exists to catch and cannot, since the name the
// operator types matches either way.
//
// Endpoints are hostnames, so case and a trailing slash carry no meaning and
// would otherwise turn a cosmetic difference into a refusal. The bucket is left
// alone: what the server considers the same bucket is the server's business.
func s3Identity(endpoint, bucket string) string {
	ep := strings.ToLower(strings.TrimSpace(endpoint))
	ep = strings.TrimSuffix(ep, "/")
	return ep + "|" + bucket
}

// describeIdentity renders a stored identity for a human. An empty endpoint
// means the AWS default, which is worth saying rather than printing "|bucket".
func describeIdentity(identity string) string {
	ep, bucket, ok := strings.Cut(identity, "|")
	if !ok {
		return fmt.Sprintf("%q", identity)
	}
	if ep == "" {
		ep = "the default AWS endpoint"
	}
	return fmt.Sprintf("bucket %q at %s", bucket, ep)
}

// checkS3Identity binds a database to its object store on first mount and
// refuses to run against a different one afterwards.
//
// This is the check the ratio guard in gc approximates. The guard notices that
// the database explains none of what is in the bucket and infers something is
// wrong; this knows. It also covers what the guard cannot: mounting a database
// against the wrong store is not a failed read but a filesystem that writes new
// data into somebody else's bucket while reporting every existing file as
// unreadable.
//
// record is true only for mount mode. A database that predates this check has no
// identity, and if a gc run were allowed to write one it would record whatever
// it was mistakenly pointed at — enshrining the error as the truth. Until the
// daemon next starts and binds it, such a database is covered by the existence
// check and the ratio guard instead.
func checkS3Identity(database *db.DB, endpoint, bucket string, record, adopt bool) error {
	want := s3Identity(endpoint, bucket)

	stored, err := database.GetConfig(s3IdentityKey)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if !record {
			// Not an error: every database created before this existed arrives
			// here exactly once per subcommand until the daemon binds it.
			log.Printf("hamstor: this database is not yet bound to a bucket; it will be bound to %s on the next mount",
				describeIdentity(want))
			return nil
		}
		if err := database.SetConfig(s3IdentityKey, []byte(want)); err != nil {
			return fmt.Errorf("record bucket identity: %w", err)
		}
		log.Printf("hamstor: database bound to %s", describeIdentity(want))
		return nil
	case err != nil:
		return fmt.Errorf("read bucket identity: %w", err)
	}

	if string(stored) == want {
		return nil
	}
	if adopt {
		if err := database.SetConfig(s3IdentityKey, []byte(want)); err != nil {
			return fmt.Errorf("rebind bucket identity: %w", err)
		}
		log.Printf("hamstor: database rebound from %s to %s (--adopt-bucket)",
			describeIdentity(string(stored)), describeIdentity(want))
		return nil
	}
	return fmt.Errorf("this database describes %s, but was pointed at %s.\n"+
		"        Its contents name objects that do not exist there: gc would see the whole\n"+
		"        bucket as unreferenced, and a mount would write into it while reporting every\n"+
		"        existing file as unreadable. Check --endpoint and --bucket; if the store really\n"+
		"        did move, re-run with --adopt-bucket",
		describeIdentity(string(stored)), describeIdentity(want))
}
