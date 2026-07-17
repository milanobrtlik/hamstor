package replicate

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"strings"
	"time"

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

	// SnapshotInterval bounds how far back a cold restore has to replay: a
	// restore starts from the most recent snapshot, so this is roughly the
	// worst-case amount of WAL a fresh machine must apply on top of it. 0 keeps
	// litestream's default (24h).
	SnapshotInterval time.Duration
	// SnapshotRetention is how long old snapshots are kept before deletion. 0
	// keeps litestream's default (24h).
	SnapshotRetention time.Duration
}

type Replicator struct {
	db      *litestream.DB
	replica *litestream.Replica
	store   *litestream.Store
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
//
// Must be called before Start: it keys on the local file being absent, and both
// Start (via the Store) and any application writer would create it.
func (r *Replicator) Restore(ctx context.Context) error {
	if _, err := os.Stat(r.config.DBPath); err == nil {
		log.Println("litestream: local DB exists, skipping restore")
		return nil
	}

	log.Println("litestream: local DB missing, attempting restore from S3...")
	opts := litestream.NewRestoreOptions()
	opts.OutputPath = r.config.DBPath

	// A cold restore fetches and replays LTX files one round-trip at a time and,
	// against a high-latency object store, can take minutes with the mount
	// blocked behind it. RestoreOptions has no progress callback, so emit our own
	// heartbeat: elapsed time plus the size of the restore's temp output. During
	// the initial plan/listing phase the temp file does not exist yet, so only
	// elapsed advances -- which is still far better than silence.
	done := make(chan struct{})
	go func() {
		start := time.Now()
		tmp := r.config.DBPath + ".tmp"
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				var mb float64
				if fi, err := os.Stat(tmp); err == nil {
					mb = float64(fi.Size()) / (1 << 20)
				}
				log.Printf("litestream: restore in progress... %s elapsed, %.1f MB written",
					time.Since(start).Round(time.Second), mb)
			}
		}
	}()

	err := r.replica.Restore(ctx, opts)
	close(done)
	if err != nil {
		// An empty replica (genuine first run) surfaces as ErrTxNotAvailable /
		// ErrNoSnapshots in litestream 0.5.x -- the plan seeder finds no snapshot
		// to start from. Treat those as "nothing to restore yet", not a failure.
		// The string checks are a defensive fallback for other/older formats.
		if errors.Is(err, litestream.ErrTxNotAvailable) || errors.Is(err, litestream.ErrNoSnapshots) ||
			strings.Contains(err.Error(), "no backups found") || strings.Contains(err.Error(), "no generations found") {
			log.Printf("litestream: no backup available (first run)")
			return nil
		}
		return fmt.Errorf("litestream restore: %w", err)
	}

	log.Println("litestream: restored DB from S3")
	return nil
}

// Start opens the database and begins replication through a litestream.Store.
//
// The Store (not a bare DB) is what runs the periodic snapshot, L0->L3
// compaction, and retention monitors. Without them the L0 WAL chain grows
// unbounded and a cold restore has to replay all of it; with them a cold restore
// is a single snapshot download plus a few compacted deltas.
func (r *Replicator) Start(ctx context.Context) error {
	store := litestream.NewStore([]*litestream.DB{r.db}, litestream.DefaultCompactionLevels)
	if r.config.SnapshotInterval > 0 {
		store.SnapshotInterval = r.config.SnapshotInterval
	}
	if r.config.SnapshotRetention > 0 {
		store.SnapshotRetention = r.config.SnapshotRetention
	}
	// Keep the shutdown sync well under main's 30s Stop budget so the
	// graceful-shutdown snapshot in Stop has room to upload. The setter
	// propagates into the underlying DB; a bare field write would not.
	store.SetShutdownSyncTimeout(10 * time.Second)

	// store.Open opens the DB itself (litestream DB.Open) and starts the monitor
	// goroutines. Do NOT also call r.db.Open() -- the Store owns the DB lifecycle.
	if err := store.Open(ctx); err != nil {
		_ = store.Close(context.Background()) // cancel any goroutines the Store started
		return fmt.Errorf("litestream store open: %w", err)
	}
	r.store = store
	log.Println("litestream: replication started (snapshots + L0->L3 compaction + retention)")
	return nil
}

// Stop writes a final full snapshot and closes the replicator.
// Must be called AFTER the application closes its database connections.
func (r *Replicator) Stop(ctx context.Context) error {
	if r.store == nil {
		return nil
	}

	// Sync first: replication is asynchronous (a ~1s monitor tick), so on a
	// short-lived run the latest WAL may not have reached the replica yet and the
	// page size may be uninitialized -- which makes Snapshot fail outright. A
	// forced sync uploads the final WAL and initializes the state the snapshot
	// needs. Safe to call while the Store's monitors run; litestream guards it.
	if err := r.db.Sync(ctx); err != nil {
		log.Printf("litestream: pre-shutdown sync failed (non-fatal): %v", err)
	}

	// Force a full snapshot at the current TXID before tearing the Store down, so
	// the next fresh-machine restore is a single snapshot download rather than an
	// L0 replay. store.Close closes the DB before cancelling its monitors, so
	// there is no post-Close window in which Snapshot can still read the file --
	// it has to run here first. The application has already closed its SQL
	// connections (main.go), so the DB is quiescent. Non-fatal: if it fails, the
	// previous periodic/startup snapshot plus deltas still restores.
	if _, err := r.db.Snapshot(ctx); err != nil {
		log.Printf("litestream: shutdown snapshot failed (non-fatal): %v", err)
	} else {
		log.Println("litestream: shutdown snapshot written")
	}

	if err := r.store.Close(ctx); err != nil {
		return fmt.Errorf("litestream store close: %w", err)
	}
	log.Println("litestream: replication stopped")
	return nil
}
