package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/milan/hamstor/internal/cache"
	"github.com/milan/hamstor/internal/creds"
	"github.com/milan/hamstor/internal/crypto"
	"github.com/milan/hamstor/internal/db"
	"github.com/milan/hamstor/internal/hfuse"
	"github.com/milan/hamstor/internal/ops"
	"github.com/milan/hamstor/internal/replicate"
	"github.com/milan/hamstor/internal/s3store"
	"github.com/milan/hamstor/internal/volume"
)

var version = "dev"

func main() {
	debug.SetMemoryLimit(150 << 20)

	mountpoint := flag.String("mount", "", "mount point (required for mount mode)")
	dbPath := flag.String("db", "data/hamstor.db", "SQLite database path")
	bucket := flag.String("bucket", "", "S3 bucket name (required)")
	endpoint := flag.String("endpoint", "", "S3 endpoint URL (for Garage/MinIO)")
	region := flag.String("region", "", "S3 region (for Garage/MinIO)")
	passphrase := flag.String("passphrase", "", "encryption passphrase (or set HAMSTOR_PASSPHRASE)")
	enableReplication := flag.Bool("replicate", true, "enable SQLite replication to S3")
	dryRun := flag.Bool("dry-run", false, "dry-run mode for gc subcommand")
	cacheDir := flag.String("cache-dir", "/var/lib/hamstor/cache", "local disk cache directory")
	cacheSizeGB := flag.Int("cache-size", 10, "max cache size in GB (0 to disable)")
	ownerUid := flag.Int("uid", os.Getuid(), "default file owner UID")
	ownerGid := flag.Int("gid", os.Getgid(), "default file owner GID")
	streamRate := flag.Int("stream-rate", 5, "streaming rate limit in MB/s for multimedia (0 to disable)")
	streamBuffer := flag.Int("stream-buffer", 16, "streaming memory buffer in MB")
	entryTimeout := flag.Duration("entry-timeout", 60*time.Second, "FUSE entry/attr cache timeout")
	pprofAddr := flag.String("pprof", "", "pprof listen address (e.g. :6060, empty to disable)")
	volumePacking := flag.Bool("volume-packing", true, "pack small files (<256KB) into volume S3 objects")
	compactRatio := flag.Float64("compact-ratio", 0.5, "dead space ratio threshold for volume compaction")
	flag.Parse()

	// Accept flags on either side of the subcommand. Go's flag package stops at
	// the first non-flag argument, so `hamstor gc --bucket x` used to parse no
	// flags at all: --bucket was silently ignored and the run died on an empty
	// bucket with a bare usage dump. Only --dry-run worked there, via a special
	// case, which made the trap worse — one flag appeared to work, implying the
	// rest did too. Re-parse whatever follows the subcommand so both orders mean
	// the same thing.
	// Collect the subcommand words, parsing flags wherever they appear. Go's
	// flag package stops at the first non-flag argument, so each positional word
	// ("cache", then "stats") ends a parse and the rest must be handed back in.
	// Without this, `hamstor gc --bucket x` parsed no flags at all: --bucket was
	// silently ignored and the run died on an empty bucket with a bare usage
	// dump. Only --dry-run worked, via a special case, which made the trap worse
	// — one flag appearing to work implies the rest do.
	subcmd := ""
	subArgs := []string{}
	for rest := flag.Args(); len(rest) > 0; rest = flag.Args() {
		if subcmd == "" {
			subcmd = rest[0]
		} else {
			subArgs = append(subArgs, rest[0])
		}
		rest = rest[1:]
		if len(rest) == 0 {
			break
		}
		// Parse consumes any flags now at the front and stops at the next
		// positional word, which the loop then takes. ExitOnError: an unknown
		// flag reports itself instead of being dropped.
		if err := flag.CommandLine.Parse(rest); err != nil {
			log.Fatalf("hamstor: %v", err)
		}
	}

	// Handle version command
	if subcmd == "version" {
		fmt.Printf("hamstor %s\n", version)
		return
	}

	// Handle commands that don't need S3
	if subcmd == "fsck" || subcmd == "cache" {
		switch subcmd {
		case "fsck":
			runFsck(*dbPath)
		case "cache":
			runCacheCmd(*cacheDir, *cacheSizeGB, subArgs)
		}
		return
	}

	if *cacheSizeGB < 0 {
		log.Fatalf("--cache-size must be >= 0")
	}
	if *bucket == "" {
		flag.Usage()
		os.Exit(1)
	}
	if subcmd == "" && *mountpoint == "" {
		flag.Usage()
		os.Exit(1)
	}

	r := *region
	if r == "" {
		r = creds.AWSRegion
	}

	if dir := filepath.Dir(*dbPath); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Fatalf("create db directory: %v", err)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Take an exclusive advisory lock on the DB so a mount and a mutating
	// subcommand (gc/compact/migrate/purge-s3/restore) cannot run against the
	// same database/bucket concurrently — which would let one delete S3 volume
	// objects out from under the other. fsck/cache/version returned earlier and
	// do not take the lock.
	lockFile, lockErr := acquireDBLock(*dbPath)
	if lockErr != nil {
		log.Fatalf("hamstor: another instance is using %s (lock: %v)", *dbPath, lockErr)
	}
	defer lockFile.Close()

	// Litestream: restore DB from S3 if local file missing, then start replication
	var rep *replicate.Replicator
	if *enableReplication {
		rep = replicate.New(replicate.Config{
			DBPath:          *dbPath,
			Bucket:          *bucket,
			Endpoint:        *endpoint,
			Region:          r,
			Path:            "litestream",
			AccessKeyID:     creds.AWSAccessKeyID,
			SecretAccessKey: creds.AWSSecretAccessKey,
		})
		if subcmd == "" || subcmd == "restore" {
			if err := rep.Restore(ctx); err != nil {
				log.Printf("litestream restore failed: %v", err)
			}
			if subcmd == "restore" {
				log.Println("restore: done")
				return
			}
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
	case "compact":
		result, err := ops.Compact(ctx, database, store, *compactRatio, *dryRun)
		if err != nil {
			log.Fatalf("compact: %v", err)
		}
		log.Printf("compact: scanned %d volumes, compacted %d, moved %d needles, reclaimed %d bytes, %d errors",
			result.VolumesScanned, result.VolumesCompacted, result.NeedlesMoved, result.BytesReclaimed, result.Errors)
		database.Close()
		return
	case "purge-s3":
		database.Close()
		runPurgeS3(ctx, store, *dbPath)
		return
	}

	// Default: mount mode
	// Order matters: CleanupVolumes removes incomplete volume records,
	// Cleanup deletes pending inodes (and their staging files become orphans),
	// CleanupStagingDir removes orphaned staging files last.
	if err := hfuse.CleanupVolumes(database, store); err != nil {
		log.Printf("hamstor: volume cleanup: %v", err)
	}
	if err := hfuse.Cleanup(database, store); err != nil {
		log.Fatalf("cleanup: %v", err)
	}
	stagingDir := filepath.Join(filepath.Dir(*dbPath), "staging")
	if err := hfuse.CleanupStagingDir(database, stagingDir); err != nil {
		log.Printf("hamstor: staging cleanup: %v", err)
	}

	// Fix ownership of root inode and any legacy uid=0/gid=0 inodes
	defaultUid := uint32(*ownerUid)
	defaultGid := uint32(*ownerGid)
	if err := database.SetOwner(1, &defaultUid, &defaultGid); err != nil {
		log.Printf("hamstor: set root owner: %v", err)
	}
	// Normalize legacy uid=0/gid=0 inodes to the configured owner ONCE, tracked
	// in the config table. Running this every boot would silently re-clobber any
	// file a user/root has since legitimately chown'd to root:root.
	if _, cfgErr := database.GetConfig("default_ownership_normalized"); errors.Is(cfgErr, sql.ErrNoRows) {
		if fixed, err := database.FixDefaultOwnership(defaultUid, defaultGid); err != nil {
			log.Printf("hamstor: fix default ownership: %v", err)
		} else {
			if fixed > 0 {
				log.Printf("hamstor: normalized ownership on %d legacy inodes", fixed)
			}
			if setErr := database.SetConfig("default_ownership_normalized", []byte("1")); setErr != nil {
				log.Printf("hamstor: record ownership normalization: %v", setErr)
			}
		}
	}

	enc := setupEncryption(database, *passphrase)

	var diskCache *cache.DiskCache
	if *cacheSizeGB > 0 {
		diskCache, err = cache.New(*cacheDir, int64(*cacheSizeGB)*1<<30)
		if err != nil {
			log.Printf("hamstor: cache init failed, continuing without cache: %v", err)
		} else {
			log.Printf("hamstor: disk cache enabled (%s, %d GB)", *cacheDir, *cacheSizeGB)
		}
	}

	// Set up spill directory and clean leftover temp files from crashes
	spillDir := filepath.Join(filepath.Dir(*dbPath), "spill")
	if err := os.MkdirAll(spillDir, 0o755); err != nil {
		log.Printf("hamstor: spill dir: %v (using system temp)", err)
		spillDir = ""
	} else {
		entries, _ := os.ReadDir(spillDir)
		for _, e := range entries {
			os.Remove(filepath.Join(spillDir, e.Name()))
		}
	}

	hfs := &hfuse.HamstorFS{
		DB: database, Store: store, Mountpoint: *mountpoint,
		Encryptor: enc, Cache: diskCache,
		DefaultUid: defaultUid, DefaultGid: defaultGid,
		StreamRate: *streamRate, StreamBuffer: *streamBuffer,
		UploadSem:    make(chan struct{}, 32),
		ThumbSem:     make(chan struct{}, 4),
		SpillDir:     spillDir,
		EntryTimeout: *entryTimeout,
		AttrTimeout:  *entryTimeout,
	}
	if *volumePacking {
		if err := os.MkdirAll(stagingDir, 0o755); err != nil {
			log.Printf("hamstor: staging dir: %v (volume packing disabled)", err)
		} else {
			hfs.VolumeBuilder = volume.NewBuilder(database, store, stagingDir)
			log.Println("hamstor: volume packing enabled (files <256KB packed into volumes)")
		}
	} else {
		// Drain any staged files left from a previous run with packing enabled.
		// Close() triggers a final scanAndSeal(true) that packs everything.
		if entries, dirErr := os.ReadDir(stagingDir); dirErr == nil && len(entries) > 0 {
			log.Printf("hamstor: volume packing disabled but %d staged files found, draining...", len(entries))
			drainBuilder := volume.NewBuilder(database, store, stagingDir)
			drainBuilder.Close()
			log.Println("hamstor: staged files drained into volumes")
		}
	}
	server, err := hfuse.Mount(*mountpoint, hfs)
	if err != nil {
		log.Fatalf("mount: %v", err)
	}

	if *pprofAddr != "" {
		go func() {
			log.Printf("hamstor: pprof listening on %s", *pprofAddr)
			if err := http.ListenAndServe(*pprofAddr, nil); err != nil {
				log.Printf("hamstor: pprof: %v", err)
			}
		}()
	}

	log.Printf("hamstor %s: mounted on %s", version, *mountpoint)

	go func() {
		<-ctx.Done()
		log.Println("hamstor: unmounting...")
		server.Unmount()
	}()

	server.Wait()

	log.Println("hamstor: waiting for in-flight uploads...")
	hfs.InflightUploads.Wait()

	if hfs.VolumeBuilder != nil {
		if err := hfs.VolumeBuilder.Close(); err != nil {
			log.Printf("hamstor: volume builder close: %v", err)
		}
	}

	database.Close()
	if rep != nil {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer stopCancel()
		if err := rep.Stop(stopCtx); err != nil {
			log.Printf("litestream stop: %v", err)
		}
	}

	log.Println("hamstor: stopped")
}

// acquireDBLock takes a non-blocking exclusive advisory lock on "<dbPath>.lock".
// The returned file must stay open for the process lifetime to hold the lock;
// the OS releases it automatically on exit. Returns an error if another process
// already holds the lock.
func acquireDBLock(dbPath string) (*os.File, error) {
	f, err := os.OpenFile(dbPath+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

func setupEncryption(database *db.DB, passphrase string) *crypto.Encryptor {
	pass := passphrase
	if pass == "" {
		pass = os.Getenv("HAMSTOR_PASSPHRASE")
	}
	if pass == "" {
		pass = creds.Passphrase
	}
	if pass == "" {
		return nil
	}

	salt, err := database.GetConfig("encryption_salt")
	if errors.Is(err, sql.ErrNoRows) {
		salt, err = crypto.GenerateSalt()
		if err != nil {
			log.Fatalf("generate encryption salt: %v", err)
		}
		if err := database.SetConfig("encryption_salt", salt); err != nil {
			log.Fatalf("store encryption salt: %v", err)
		}
		log.Println("hamstor: encryption enabled (new salt generated)")
	} else if err != nil {
		log.Fatalf("read encryption salt: %v", err)
	} else {
		log.Println("hamstor: encryption enabled")
	}
	enc, err := crypto.New(pass, salt)
	if err != nil {
		log.Fatalf("init encryption: %v", err)
	}
	return enc
}

func runFsck(dbPath string) {
	if dir := filepath.Dir(dbPath); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Fatalf("create db directory: %v", err)
		}
	}
	database, err := db.Open(dbPath)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer database.Close()

	result, err := database.Fsck()
	if err != nil {
		log.Fatalf("fsck: %v", err)
	}

	fmt.Printf("fsck results:\n")
	fmt.Printf("  total inodes:       %d\n", result.TotalInodes)
	fmt.Printf("  orphaned inodes:    %d\n", result.OrphanedInodes)
	fmt.Printf("  pending inodes:     %d\n", result.PendingInodes)
	fmt.Printf("  staged files:       %d\n", result.StagedFiles)
	fmt.Printf("  sealed volumes:     %d\n", result.VolumeCount)
	fmt.Printf("  volume mismatches:  %d\n", result.VolumeMismatches)

	if result.OrphanedInodes > 0 || result.PendingInodes > 0 {
		fmt.Println("  status: ISSUES FOUND (run gc to clean up)")
		os.Exit(1)
	}
	if result.VolumeMismatches > 0 {
		fmt.Println("  status: ISSUES FOUND (volume stats inconsistent)")
		os.Exit(1)
	}
	if result.StagedFiles > 0 {
		fmt.Println("  status: OK (staged files will be packed into volumes)")
	} else {
		fmt.Println("  status: OK")
	}
}

func runCacheCmd(cacheDir string, cacheSizeGB int, args []string) {
	if cacheSizeGB <= 0 {
		fmt.Println("cache is disabled (--cache-size=0)")
		return
	}
	c, err := cache.New(cacheDir, int64(cacheSizeGB)*1<<30)
	if err != nil {
		log.Fatalf("open cache: %v", err)
	}

	subcmd := "stats"
	if len(args) > 0 {
		subcmd = args[0]
	}

	switch subcmd {
	case "stats":
		totalBytes, count := c.Size()
		fmt.Printf("cache: %s\n", cacheDir)
		fmt.Printf("  entries: %d\n", count)
		fmt.Printf("  size:    %.1f MB\n", float64(totalBytes)/(1<<20))
		fmt.Printf("  limit:   %d GB\n", cacheSizeGB)
	case "clear":
		if err := c.Clear(); err != nil {
			log.Fatalf("cache clear: %v", err)
		}
		fmt.Println("cache cleared")
	default:
		fmt.Fprintf(os.Stderr, "unknown cache subcommand: %s (use: stats, clear)\n", subcmd)
		os.Exit(1)
	}
}

func runPurgeS3(ctx context.Context, store *s3store.Store, dbPath string) {
	keys, err := store.List(ctx, "")
	if err != nil {
		log.Fatalf("purge-s3: list S3 objects: %v", err)
	}
	log.Printf("purge-s3: deleting %d S3 objects...", len(keys))
	deleted, err := store.DeleteBatch(ctx, keys)
	if err != nil {
		log.Printf("purge-s3: batch delete error: %v", err)
	}

	// Remove local DB and WAL/SHM files
	for _, suffix := range []string{"", "-wal", "-shm"} {
		os.Remove(dbPath + suffix)
	}

	log.Printf("purge-s3: done (%d objects deleted)", deleted)
	if err != nil {
		os.Exit(1)
	}
}
