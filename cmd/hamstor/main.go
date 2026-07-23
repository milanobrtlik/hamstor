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
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
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
	dryRun := flag.Bool("dry-run", false, "dry-run mode for gc and purge-s3 subcommands")
	assumeYes := flag.Bool("yes", false, "skip the purge-s3 confirmation prompt (for scripts)")
	cacheDir := flag.String("cache-dir", "/var/lib/hamstor/cache", "local disk cache directory")
	cacheSizeGB := flag.Int("cache-size", 10, "max cache size in GB (0 to disable)")
	ownerUid := flag.Int("uid", os.Getuid(), "default file owner UID")
	ownerGid := flag.Int("gid", os.Getgid(), "default file owner GID")
	streamRate := flag.Int("stream-rate", 5, "streaming rate limit in MB/s for multimedia (0 to disable)")
	streamBuffer := flag.Int("stream-buffer", 16, "streaming memory buffer in MB")
	writeBuffer := flag.Int64("write-buffer", 1<<30, "max local un-uploaded write buffer in bytes; Write blocks past it so a bulk copy paces to the S3 upload rate and the spill dir stays bounded (0 to disable). A single file larger than this still needs local disk equal to its size.")
	entryTimeout := flag.Duration("entry-timeout", 60*time.Second, "FUSE entry/attr cache timeout")
	pprofAddr := flag.String("pprof", "", "pprof listen address (e.g. :6060, empty to disable)")
	volumePacking := flag.Bool("volume-packing", true, "pack small files (<256KB) into volume S3 objects")
	compactRatio := flag.Float64("compact-ratio", 0.5, "dead space ratio threshold for volume compaction")
	snapshotInterval := flag.Duration("snapshot-interval", 6*time.Hour, "litestream snapshot interval; lower = faster cold restore, higher = less B2 cost")
	shutdownTimeout := flag.Duration("shutdown-timeout", 10*time.Second, "budget for the waiting parts of shutdown; retention of interrupted uploads is never cut short")
	flag.Parse()

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
	// subcommand (gc/compact/purge-s3/restore) cannot run against the
	// same database/bucket concurrently — which would let one delete S3 volume
	// objects out from under the other. fsck/cache/version returned earlier and
	// do not take the lock.
	lockFile, lockErr := acquireDBLock(*dbPath)
	if errors.Is(lockErr, errLockHeld) {
		log.Fatalf("hamstor: another instance is using %s", *dbPath)
	} else if lockErr != nil {
		log.Fatalf("hamstor: cannot lock %s: %v", *dbPath, lockErr)
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

			SnapshotInterval:  *snapshotInterval,
			SnapshotRetention: 24 * time.Hour,
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
		runPurgeS3(ctx, store, *dbPath, *bucket, *endpoint, *dryRun, *assumeYes)
		return
	}

	// pendingDir holds bytes from uploads that failed in an earlier run. Unlike
	// spillDir it is never wiped: it is the only remaining copy of that data.
	pendingDir := filepath.Join(filepath.Dir(*dbPath), "pending")
	if err := os.MkdirAll(pendingDir, 0o755); err != nil {
		log.Printf("hamstor: pending dir: %v (failed uploads will not be recoverable)", err)
		pendingDir = ""
	}

	// Default: mount mode
	// Order matters: CleanupVolumes removes incomplete volume records,
	// RecoverPending re-uploads what it can BEFORE Cleanup deletes the rest,
	// Cleanup deletes pending inodes (and their staging files become orphans),
	// CleanupStagingDir removes orphaned staging files last.
	if err := hfuse.CleanupVolumes(database, store); err != nil {
		log.Printf("hamstor: volume cleanup: %v", err)
	}
	if err := hfuse.RecoverPending(database, store, pendingDir); err != nil {
		log.Printf("hamstor: recover pending uploads: %v", err)
	}
	if err := hfuse.Cleanup(database, store, pendingDir); err != nil {
		log.Fatalf("cleanup: %v", err)
	}
	stagingDir := filepath.Join(filepath.Dir(*dbPath), "staging")
	if err := hfuse.CleanupStagingDir(database, stagingDir); err != nil {
		log.Printf("hamstor: staging cleanup: %v", err)
	}

	// Committed files whose bytes are neither in S3 nor in staging read as EIO
	// forever. The usual cause is a DB restored onto a host that does not have
	// the original staging disk. Say so at mount time instead of letting it be
	// discovered one failed read at a time.
	if missing, err := hfuse.CheckStagedData(database, stagingDir); err != nil {
		log.Printf("hamstor: staged data check: %v", err)
	} else if len(missing) > 0 {
		log.Printf("hamstor: WARNING: %d committed file(s) have no data in S3 and no staging file — reads will fail:", len(missing))
		for i, meta := range missing {
			if i == 10 {
				log.Printf("hamstor:   ... and %d more (run `hamstor fsck` for the full list)", len(missing)-10)
				break
			}
			log.Printf("hamstor:   unreadable: %s (inode %d, %d bytes)", meta.Name, meta.ID, meta.Size)
		}
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

	// Uploads outlive the close(2) that starts them, so they cannot inherit a
	// FUSE request's context. This one exists so shutdown can interrupt them:
	// waiting instead is unbounded, and a cancelled upload retains its bytes and
	// is finished by RecoverPending on the next start.
	uploadCtx, cancelUploads := context.WithCancel(context.Background())
	defer cancelUploads()

	hfs := &hfuse.HamstorFS{
		DB: database, Store: store, Mountpoint: *mountpoint,
		Encryptor: enc, Cache: diskCache,
		DefaultUid: defaultUid, DefaultGid: defaultGid,
		StreamRate: *streamRate, StreamBuffer: *streamBuffer,
		WriteBuffer: *writeBuffer,
		UploadCtx:   uploadCtx,
		UploadSem: make(chan struct{}, 32),
		// Four block encryptions at once: each holds a block's plaintext and its
		// sealed copy, so 4 * 2 * 8 MiB = 64 MiB, comfortably inside the 150 MiB
		// limit set above alongside the write buffers (one block each now). The
		// unencrypted path streams off the snapshot and never comes here, so this
		// does not cap a plain mount's upload concurrency at 4.
		EncryptSem:   make(chan struct{}, 4),
		ThumbSem:     make(chan struct{}, 4),
		SpillDir:     spillDir,
		PendingDir:   pendingDir,
		EntryTimeout: *entryTimeout,
		AttrTimeout:  *entryTimeout,
	}
	if *volumePacking {
		if err := os.MkdirAll(stagingDir, 0o755); err != nil {
			log.Printf("hamstor: staging dir: %v (volume packing disabled)", err)
		} else {
			hfs.VolumeBuilder = volume.NewBuilder(database, store, stagingDir, diskCache)
			// One budget for the whole shutdown: the builder's own default would
			// otherwise add its 30s on top of it.
			hfs.VolumeBuilder.CloseTimeout = *shutdownTimeout
			log.Println("hamstor: volume packing enabled (files <256KB packed into volumes)")
		}
	} else {
		// Drain any staged files left from a previous run with packing enabled.
		// Close() triggers a final scanAndSeal(true) that packs everything.
		if entries, dirErr := os.ReadDir(stagingDir); dirErr == nil && len(entries) > 0 {
			log.Printf("hamstor: volume packing disabled but %d staged files found, draining...", len(entries))
			drainBuilder := volume.NewBuilder(database, store, stagingDir, diskCache)
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

	// server.Wait() is NOT the main line of control. It used to be, with the
	// unmount pushed into a goroutine whose error was dropped — and go-fuse's
	// Unmount gives up after five fusermount attempts inside ~75 ms
	// (fuse/server.go), which fails whenever anything holds the mount busy. A
	// shell with its cwd inside is enough, and at machine shutdown that is the
	// normal case, not an edge one. Wait() then never returned, systemd timed the
	// stop out and SIGABRTed the daemon mid-upload with nothing retained.
	served := make(chan struct{})
	go func() {
		server.Wait()
		close(served)
	}()

	<-ctx.Done()
	deadline := time.Now().Add(*shutdownTimeout)

	if lazy := releaseMount(server, *mountpoint); lazy {
		// A lazy detach has already taken the mount out of the namespace, so
		// nothing can reach the filesystem any more and there is nothing left to
		// wait for. The connection itself stays up until whoever held it closes
		// their descriptors, which may be never — waiting on that is what the old
		// shutdown effectively did.
		log.Println("hamstor: mount detached; open descriptors drain on their own")
	} else if !waitClosed(served, time.Until(deadline)) {
		log.Printf("hamstor: FUSE connection still held after %v, continuing shutdown", *shutdownTimeout)
	}

	// A short grace, then cancel. An upload seconds from done beats re-sending
	// it, but waiting for one to finish is unbounded — at the few MB/s a home
	// uplink gives, one large file is minutes. Cancelling lands in the retention
	// path, so the bytes survive as a pending set and the next start uploads them.
	if hfs.InflightCount.Load() > 0 &&
		!waitGroup(&hfs.InflightUploads, min(uploadGrace, time.Until(deadline))) {
		log.Printf("hamstor: cancelling %d in-flight upload(s); their bytes are retained for the next start",
			hfs.InflightCount.Load())
		cancelUploads()
		// Deliberately unbounded: what runs now is local-disk retention, and that
		// is the only thing standing between an interrupted write and losing it.
		// Cutting it off mid-set would be strictly worse than not starting —
		// RecoverPending refuses a half-written set, so the time would be spent
		// and the data lost anyway.
		hfs.InflightUploads.Wait()
	}

	if hfs.VolumeBuilder != nil {
		if err := hfs.VolumeBuilder.Close(); err != nil {
			log.Printf("hamstor: volume builder close: %v", err)
		}
	}

	database.Close()
	if rep != nil {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), *shutdownTimeout)
		defer stopCancel()
		if err := rep.Stop(stopCtx); err != nil {
			log.Printf("litestream stop: %v", err)
		}
	}

	log.Println("hamstor: stopped")
}

// uploadGrace is how long shutdown lets in-flight uploads finish on their own
// before cancelling them. Short on purpose: it is here to catch the small file
// that is nearly done, not to wait out a large one. A cancelled upload is re-sent
// in full on the next start, which costs nothing but time on B2 (Class A
// requests are free).
const uploadGrace = 2 * time.Second

// releaseMount detaches the filesystem, and unlike a bare server.Unmount() it
// cannot leave the daemon holding a mount nobody can get rid of.
//
// go-fuse tries fusermount five times over ~75 ms and then returns EBUSY for
// good — it never retries later, so once anything has held the mount busy at
// that instant the daemon stays wedged even after the holder goes away
// (reproduced: the process outlived its own holder and only exited when a lazy
// unmount was issued by hand).
//
// So the fallback is a LAZY detach, which is the right answer for shutdown
// anyway: it removes the mount from the namespace immediately, so nothing new
// can enter while open descriptors drain. It is also exactly what the unit file
// already does at the other end, with `umount -l` in ExecStartPre.
//
// Reports whether it had to fall back, because that changes what the caller
// should do next: after a lazy detach the FUSE connection stays up until the
// holder closes its descriptors, so waiting for it buys nothing.
func releaseMount(server *fuse.Server, mountpoint string) (lazy bool) {
	log.Println("hamstor: unmounting...")
	err := server.Unmount()
	if err == nil {
		return false
	}
	log.Printf("hamstor: unmount: %s — detaching lazily",
		strings.TrimSpace(strings.ReplaceAll(err.Error(), "\n", " ")))

	// MNT_DETACH first: the daemon runs as root under systemd, and this needs no
	// subprocess. fusermount -uz covers an unprivileged run, where the mount's
	// owner may unmount it but umount2 is refused.
	if uErr := syscall.Unmount(mountpoint, syscall.MNT_DETACH); uErr == nil {
		return true
	}
	if out, fErr := exec.Command("fusermount", "-u", "-z", mountpoint).CombinedOutput(); fErr != nil {
		log.Printf("hamstor: lazy unmount of %s failed: %v: %s", mountpoint, fErr, out)
	}
	return true
}

// waitClosed reports whether c closed within d. A non-positive d still gives it
// one non-blocking look, so an already-finished wait is never reported as a
// timeout just because the budget ran out elsewhere.
func waitClosed(c <-chan struct{}, d time.Duration) bool {
	if d <= 0 {
		select {
		case <-c:
			return true
		default:
			return false
		}
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-c:
		return true
	case <-timer.C:
		return false
	}
}

// waitGroup is waitClosed for a WaitGroup: reports whether it reached zero
// within d. The goroutine it leaks when it does not is bounded by the number of
// shutdown phases and dies with the process.
func waitGroup(wg *sync.WaitGroup, d time.Duration) bool {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	return waitClosed(done, d)
}

// errLockHeld reports the one failure that actually means a second hamstor is
// running. Every other way of failing to take the lock — the file cannot be
// opened, the directory does not exist, the filesystem refuses flock — says
// nothing about other processes, and telling the user to go find one sends them
// after something that is not there. The common case is a permission error:
// systemd runs the daemon as root, so /var/lib/hamstor and its lock file belong
// to root, and a user running `hamstor gc` cannot open it.
var errLockHeld = errors.New("lock held by another process")

// acquireDBLock takes a non-blocking exclusive advisory lock on "<dbPath>.lock".
// The returned file must stay open for the process lifetime to hold the lock;
// the OS releases it automatically on exit. Returns errLockHeld if another
// process already holds it, and a descriptive error for anything else.
func acquireDBLock(dbPath string) (*os.File, error) {
	lockPath := dbPath + ".lock"
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", lockPath, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		// LOCK_NB reports contention as EWOULDBLOCK (== EAGAIN); anything else
		// is the filesystem or the descriptor refusing, not a rival process.
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, errLockHeld
		}
		return nil, fmt.Errorf("flock %s: %w", lockPath, err)
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

	// A staged file is only fine while its bytes are on this disk. Check rather
	// than assume: reporting "staged files will be packed into volumes" for an
	// inode whose staging file does not exist calls permanent data loss OK.
	stagingDir := filepath.Join(filepath.Dir(dbPath), "staging")
	missing, err := hfuse.CheckStagedData(database, stagingDir)
	if err != nil {
		log.Fatalf("fsck: staged data check: %v", err)
	}

	fmt.Printf("fsck results:\n")
	fmt.Printf("  total inodes:       %d\n", result.TotalInodes)
	fmt.Printf("  orphaned inodes:    %d\n", result.OrphanedInodes)
	fmt.Printf("  pending inodes:     %d\n", result.PendingInodes)
	fmt.Printf("  staged files:       %d\n", result.StagedFiles-len(missing))
	fmt.Printf("  unreadable files:   %d\n", len(missing))
	fmt.Printf("  sealed volumes:     %d\n", result.VolumeCount)
	fmt.Printf("  volume mismatches:  %d\n", result.VolumeMismatches)

	if len(missing) > 0 {
		fmt.Println("\n  These files are committed but their data is neither in S3 nor in")
		fmt.Printf("  %s — reads return EIO and no retry will fix it:\n", stagingDir)
		for _, meta := range missing {
			fmt.Printf("    %s (inode %d, %d bytes)\n", meta.Name, meta.ID, meta.Size)
		}
		fmt.Println("\n  status: ISSUES FOUND (unreadable files)")
		os.Exit(1)
	}
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

// runPurgeS3 deletes every object in the bucket and the local DB. It is the one
// irreversible command here and there is no undo, so it asks before doing it:
// the bucket name must be typed back. Production and test buckets are commonly
// named alike and differ only by endpoint, so a purge aimed at the wrong
// endpoint destroys everything without the prompt to catch it. --yes skips the
// prompt for scripts; --dry-run reports and changes nothing.
func runPurgeS3(ctx context.Context, store *s3store.Store, dbPath, bucket, endpoint string, dryRun, assumeYes bool) {
	keys, err := store.List(ctx, "")
	if err != nil {
		log.Fatalf("purge-s3: list S3 objects: %v", err)
	}

	target := bucket
	if endpoint != "" {
		target = fmt.Sprintf("%s (%s)", bucket, endpoint)
	}

	if dryRun {
		log.Printf("purge-s3: would delete %d S3 objects from %s and remove %s — dry run, nothing changed", len(keys), target, dbPath)
		return
	}

	if !assumeYes {
		fmt.Fprintf(os.Stderr, "purge-s3: about to permanently delete ALL %d objects in bucket %s\n"+
			"and remove the local database %s. This cannot be undone.\n"+
			"Type the bucket name to confirm: ", len(keys), target, dbPath)
		var answer string
		// A non-interactive stdin yields an error here and aborts, which is the
		// safe direction: never purge because nobody was there to say no.
		if _, err := fmt.Scanln(&answer); err != nil || answer != bucket {
			log.Fatalf("purge-s3: aborted (bucket name not confirmed)")
		}
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
