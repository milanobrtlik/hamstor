package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/milan/hamstor/internal/creds"
	"github.com/milan/hamstor/internal/crypto"
	"github.com/milan/hamstor/internal/db"
	"github.com/milan/hamstor/internal/hfuse"
	"github.com/milan/hamstor/internal/ops"
	"github.com/milan/hamstor/internal/replicate"
	"github.com/milan/hamstor/internal/s3store"
)

func main() {
	mountpoint := flag.String("mount", "", "mount point (required for normal operation)")
	dbPath := flag.String("db", "data/hamstor.db", "SQLite database path")
	bucket := flag.String("bucket", "", "S3 bucket name (required)")
	endpoint := flag.String("endpoint", "", "S3 endpoint URL (for Garage/MinIO)")
	region := flag.String("region", "", "S3 region (for Garage/MinIO)")
	passphrase := flag.String("passphrase", "", "encryption passphrase (or set HAMSTOR_PASSPHRASE)")
	enableReplication := flag.Bool("replicate", true, "enable SQLite replication to S3")
	dryRun := flag.Bool("dry-run", false, "dry-run mode for gc subcommand")
	flag.Parse()

	subcmd := ""
	if args := flag.Args(); len(args) > 0 {
		subcmd = args[0]
		for _, a := range args[1:] {
			if a == "--dry-run" || a == "-dry-run" {
				*dryRun = true
			}
		}
	}

	if *bucket == "" {
		flag.Usage()
		os.Exit(1)
	}
	if subcmd == "" && *mountpoint == "" {
		flag.Usage()
		os.Exit(1)
	}

	// Resolve region: flag > embedded > empty (SDK default chain)
	r := *region
	if r == "" {
		r = creds.AWSRegion
	}

	// Ensure DB parent directory exists
	if dir := filepath.Dir(*dbPath); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Fatalf("create db directory: %v", err)
		}
	}

	ctx := context.Background()

	// Litestream: restore DB from S3 if local file missing, then start replication
	var rep *replicate.Replicator
	if *enableReplication && subcmd == "" {
		rep = replicate.New(replicate.Config{
			DBPath:          *dbPath,
			Bucket:          *bucket,
			Endpoint:        *endpoint,
			Region:          r,
			Path:            "litestream",
			AccessKeyID:     creds.AWSAccessKeyID,
			SecretAccessKey: creds.AWSSecretAccessKey,
		})
		if err := rep.Restore(ctx); err != nil {
			log.Printf("litestream restore failed: %v", err)
		}
		if err := rep.Start(ctx); err != nil {
			log.Printf("litestream start failed, continuing without replication: %v", err)
			rep = nil
		}
	}

	database, err := db.Open(*dbPath)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}

	store, err := s3store.New(ctx, *bucket, *endpoint, creds.AWSAccessKeyID, creds.AWSSecretAccessKey, r)
	if err != nil {
		log.Fatalf("create s3 store: %v", err)
	}

	switch subcmd {
	case "migrate":
		if err := ops.Migrate(ctx, database, store); err != nil {
			log.Fatalf("migrate: %v", err)
		}
		database.Close()
		return
	case "gc":
		result, err := ops.GC(ctx, database, store, *dryRun)
		if err != nil {
			log.Fatalf("gc: %v", err)
		}
		log.Printf("gc: %d s3 orphans, %d db orphans, %d deleted, %d errors", result.OrphansFound, result.DBOrphans, result.OrphansDeleted, result.Errors)
		database.Close()
		return
	}

	if err := hfuse.Cleanup(database, store); err != nil {
		log.Fatalf("cleanup: %v", err)
	}

	// Encryption setup: flag > env > embedded
	var enc *crypto.Encryptor
	pass := *passphrase
	if pass == "" {
		pass = os.Getenv("HAMSTOR_PASSPHRASE")
	}
	if pass == "" {
		pass = creds.Passphrase
	}
	if pass != "" {
		salt, err := database.GetConfig("encryption_salt")
		if err != nil {
			salt, err = crypto.GenerateSalt()
			if err != nil {
				log.Fatalf("generate encryption salt: %v", err)
			}
			if err := database.SetConfig("encryption_salt", salt); err != nil {
				log.Fatalf("store encryption salt: %v", err)
			}
			log.Println("hamstor: encryption enabled (new salt generated)")
		} else {
			log.Println("hamstor: encryption enabled")
		}
		enc, err = crypto.New(pass, salt)
		if err != nil {
			log.Fatalf("init encryption: %v", err)
		}
	}

	hfs := &hfuse.HamstorFS{DB: database, Store: store, Mountpoint: *mountpoint, Encryptor: enc}
	server, err := hfuse.Mount(*mountpoint, hfs)
	if err != nil {
		log.Fatalf("mount: %v", err)
	}

	log.Printf("hamstor: mounted on %s", *mountpoint)

	// Wait for shutdown signal
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-ch
		log.Println("hamstor: unmounting...")
		server.Unmount()
	}()

	server.Wait()

	// Shutdown: close DB before stopping replication (critical ordering)
	database.Close()
	if rep != nil {
		if err := rep.Stop(ctx); err != nil {
			log.Printf("litestream stop: %v", err)
		}
	}

	log.Println("hamstor: stopped")
}
