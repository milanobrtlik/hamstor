package replicate

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/s3"
)

type Config struct {
	DBPath          string
	Bucket          string
	Endpoint        string
	Region          string // S3 region, default "us-east-1"
	Path            string // S3 key prefix, default "litestream"
	AccessKeyID     string // optional, for build-time embedded creds
	SecretAccessKey string // optional, for build-time embedded creds
}

type Replicator struct {
	db      *litestream.DB
	replica *litestream.Replica
	config  Config
}

func New(cfg Config) *Replicator {
	db := litestream.NewDB(cfg.DBPath)
	db.Logger = slog.Default()

	client := s3.NewReplicaClient()
	client.Bucket = cfg.Bucket
	client.Path = cfg.Path
	client.Endpoint = cfg.Endpoint
	client.Region = cfg.Region
	client.ForcePathStyle = true
	client.AccessKeyID = cfg.AccessKeyID
	client.SecretAccessKey = cfg.SecretAccessKey

	replica := litestream.NewReplicaWithClient(db, client)
	db.Replica = replica

	return &Replicator{
		db:      db,
		replica: replica,
		config:  cfg,
	}
}

// Restore downloads the database from S3 if the local file does not exist.
// Returns nil if the local file already exists or if no replica is available (first run).
func (r *Replicator) Restore(ctx context.Context) error {
	if _, err := os.Stat(r.config.DBPath); err == nil {
		log.Println("litestream: local DB exists, skipping restore")
		return nil
	}

	log.Println("litestream: local DB missing, attempting restore from S3...")
	opts := litestream.NewRestoreOptions()
	opts.OutputPath = r.config.DBPath

	if err := r.replica.Restore(ctx, opts); err != nil {
		log.Printf("litestream: restore failed (first run?): %v", err)
		return nil
	}

	log.Println("litestream: restored DB from S3")
	return nil
}

// Start begins WAL monitoring and background replication.
func (r *Replicator) Start(ctx context.Context) error {
	if err := r.db.Open(); err != nil {
		return fmt.Errorf("litestream open: %w", err)
	}
	log.Println("litestream: replication started")
	return nil
}

// Stop performs a final sync and closes the replicator.
// Must be called AFTER the application closes its database connections.
func (r *Replicator) Stop(ctx context.Context) error {
	if err := r.db.SyncAndWait(ctx); err != nil {
		log.Printf("litestream: final sync failed: %v", err)
	}
	if err := r.db.Close(ctx); err != nil {
		return fmt.Errorf("litestream close: %w", err)
	}
	log.Println("litestream: replication stopped")
	return nil
}
