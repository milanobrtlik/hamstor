# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

Hamstor is a FUSE filesystem daemon (Go 1.25+) that presents S3-compatible object storage as a local mountable filesystem. Files are buffered locally (memory for small files, temp files for large), uploaded to S3 on flush, with metadata stored in SQLite. Litestream replicates the database to S3 for recovery.

## Commands

```bash
make build              # Build binary (requires .env with S3 credentials)
source .env.test && go test ./...           # Run all tests (S3 tests need .env.test)
source .env.test && go test ./internal/hfuse/  # Run tests for a single package
go vet ./...            # Lint
./hamstor --mount /path --bucket name --endpoint https://s3.example.com  # Run manually
./hamstor version       # Show version
./hamstor fsck          # Check filesystem consistency
./hamstor cache stats   # Show cache statistics
./hamstor cache clear   # Clear disk cache
./hamstor gc            # Garbage collect orphaned S3 objects (--dry-run to preview)
./hamstor compact       # Rewrite volumes with high dead space (--compact-ratio)
./hamstor migrate       # Migrate legacy unprefixed S3 keys to {2-hex}/{uuid}
./hamstor restore       # Restore DB from S3 via Litestream
./hamstor purge-s3      # DESTRUCTIVE: delete every object in the bucket + local DB
```

Flags work on either side of the subcommand (`hamstor gc --bucket x` and `hamstor --bucket x gc` are equivalent) — `main` re-parses around each positional word, because Go's `flag` package otherwise stops at the subcommand and silently ignores everything after it.

Build embeds S3 credentials and passphrase via ldflags from `.env` (see `.env.example`).

Tests requiring S3 (upload, download, GC, range reads) need credentials from `.env.test` (`source .env.test` before `go test`) and a reachable S3 endpoint — by default a local Garage on `http://localhost:3900` with the `hamstor` bucket. `testutil.RequireS3` probes both and calls `t.Skip` when either is missing, so an unconfigured checkout skips those tests in milliseconds instead of failing after ~30s of SDK retries.

## Architecture

**Data flow:** Write → memory buffer or spill file → async S3 upload on Flush → SQLite metadata commit → old S3 key deleted if update.

**Read path:** Check disk cache → S3 range request (unencrypted) → full S3 download + decrypt → cache on disk.

**Crash safety:** Files start as `status='pending'` in SQLite. On successful S3 upload they become `'committed'`. On startup `RecoverPending` finishes uploads that failed in an earlier run, then `Cleanup` deletes the pending entries that are left. GC removes orphaned S3 objects.

**Failed-upload recovery:** When an async upload fails, the bytes it meant to send are retained under `<db-dir>/pending/<inodeID>.<logicalSize>` and the inode stays `'pending'`. `RecoverPending` uploads them verbatim on the next start (they are already ciphertext under encryption, so it never needs the passphrase) and commits with the logical size from the filename — the stored object is longer than the file it represents when encrypted. Three rules hold this together, and breaking any one reintroduces silent data loss:
- `<db-dir>/pending/` must **never** be wiped at startup the way `spill/` is; it is the only copy of that data.
- `Cleanup` must skip pending inodes that still have retained bytes, and must run **after** `RecoverPending`.
- A recovery attempt that cannot reach S3 leaves the file in place to retry, and never deletes it.

```
cmd/hamstor/main.go        Entry point, wires everything together
internal/hfuse/            FUSE implementation (go-fuse v2 fs.Node interface)
  fs.go                    HamstorFS - filesystem root, holds DB/S3/crypto/cache refs
  node.go                  HamstorNode - inode ops (Lookup, Readdir, Mkdir, Create, Unlink, Rename, Setattr, Symlink, Xattr)
  handle.go                HamstorHandle - file handle (Read/Write/Flush with cache and spill-to-disk)
  writestate.go            inodeWrite - per-inode write state shared by all handles on an inode, + its registry
  cleanup.go               Crash recovery on startup
internal/cache/            Disk cache with LRU eviction, keyed by S3 key
internal/db/               SQLite metadata (inodes + xattrs + config tables)
internal/s3store/          S3 client wrapper with retry logic; supports range requests
internal/crypto/           Optional AES-256-GCM encryption with Argon2id key derivation
internal/replicate/        Litestream integration for DB replication to S3
internal/ops/              GC (orphan cleanup) and S3 key migration
internal/thumb/            Freedesktop thumbnail generation for images
internal/volume/           Packs small files into volume objects (builder + staging dir)
internal/creds/            Build-time embedded credentials via ldflags
internal/testutil/         Test helper: RequireS3 (config + reachability probe, skips when absent)
```

## Key Patterns

- **S3 keys** are prefixed: `{first-2-hex}/{uuid}` (e.g., `a1/a1b2c3d4-...`). Legacy unprefixed keys can be migrated via `--migrate`.
- **Encryption** is optional (passphrase from flag > env > embedded). Format: `[version byte][12-byte nonce][ciphertext+tag]`. `IsEncrypted()` checks the version byte.
- **Async uploads:** `Flush` launches a goroutine for S3 upload, updates DB on completion. This enables parallel file copies. Because a file's size only lands at `CommitInode`, the async path must invalidate the kernel's cached attributes afterwards (`inode.NotifyContent(-1, 0)` — a negative offset means attributes only, leaving the page cache alone). Without it `ls -l` reports 0 bytes for up to `--entry-timeout` (60s) after a `cp`.
- **Shared per-inode write state:** every open handle on one inode shares an `inodeWrite` (`internal/hfuse/writestate.go`), refcounted in `HamstorFS.writeStates`. Giving each handle its own buffer is what let two concurrent writers erase each other — each snapshotted the whole file and the last `Flush` wrote its snapshot back wholesale. Four rules hold this together:
  - **`writeMu` is a leaf lock.** It guards the map and the refcounts, nothing else. Never take `inodeWrite.mu` — or make a syscall, S3 or DB call — while holding it. `acquireWrite`/`tryAcquireWrite` return with it released, which is what lets `Flush` bump `uploadRefs` while already holding `st.mu` without inverting the order. A `truncate` racing a `close` would otherwise wedge the whole mount.
  - **The upload goroutine never takes `st.mu`.** That is what makes it safe for `Flush` and `ensureLoaded` to release the lock and wait on an attempt. It communicates via an immutable `uploadAttempt` (`err` written only before `close(done)`); do not go back to recycling shared `uploadDone`/`uploadErr` fields, which is a race the moment they are shared.
  - **The attempt is published before `Flush` first releases `st.mu`,** and `oldKey` is read only *after* the wait for a previous attempt. Publishing late reopens a seconds-wide window where a concurrent load takes the about-to-be-replaced key as its base; reading `oldKey` early makes the losing flush delete the winner's live object.
  - **A failed attempt poisons the state** (every later op returns `EIO`) until the last ref goes. The retained bytes under `<db-dir>/pending/` are then the only copy, and a sibling handle that quietly committed over them would flip the inode to `'committed'` — at which point `RecoverPending` deletes them as stale. See "Failed-upload recovery".
  Both the async and the volume-staging paths publish an attempt; staging returns early, so without one it races the async path outright.
- **`O_APPEND` is honored by the server, not the kernel:** `Write` ignores the offset it is handed for append-mode handles and appends at the true shared end of file. The kernel derives append offsets from its cached `st_size` (attr timeout, 60s), so two appenders get the same stale offset and one overwrites the other even with a shared buffer. Linux already ignores the offset for `pwrite()` on an `O_APPEND` fd, so this matches it.
- **`fileSize` is per-handle on purpose:** it is a per-handle *view* of shared data, needed by `readChunked`/`readStreaming` to match the `s3Key` the handle opened against. `readLoaded` only clamps to it for clean reads, and any write or truncate sets `st.dirty`, so a stale `fileSize` cannot truncate a sibling's writes. Moving it into the shared state breaks the chunked read path.
- **Thumbnails** are generated from the plaintext *on disk* (the spill file, or a temp copy for staged small files), never from a heap buffer, and a bounded worker pool reads each source only after taking a slot. Holding the image in a queued goroutine instead puts one full-size buffer per pending file in RAM. Generation is gated on that path existing — do not re-gate it on an in-memory buffer, which is how it silently broke for unencrypted mounts once uploads started streaming from disk.
- **Disk cache:** Downloaded/decrypted files are cached on local disk (`--cache-dir`). Keyed by S3 key (changes per version = automatic invalidation). LRU eviction with `--cache-size` limit.
- **Range requests:** For unencrypted files not yet in cache, reads use S3 Range headers instead of downloading the entire file. A background goroutine warms the cache.
- **Spill to disk:** Writes larger than 64 MB use a temp file instead of in-memory buffer, avoiding OOM on large files.
- **S3 retries:** All S3 operations retry up to 3 times with exponential backoff on transient failures.
- **UID/GID:** Stored in DB, set to caller's identity on creation, modifiable via chown.
- **Symlinks:** Stored as inodes with `symlink_target` column, mode `S_IFLNK`.
- **Extended attributes:** Stored in `xattrs` table (inode_id, name, value).
- **FUSE error mapping:** `sql.ErrNoRows` → `ENOENT`, SQLite UNIQUE constraint → `EEXIST`, other errors → `EIO`.
- **SQLite uses modernc.org/sqlite** (pure Go, no CGO). WAL mode, busy timeout 5s. Foreign key on `xattrs.inode_id` with CASCADE delete.
- **Litestream** runs embedded (not as a separate process), replicating to `litestream/` prefix in the same S3 bucket.
- **Graceful shutdown:** On SIGINT/SIGTERM, waits for all in-flight async uploads before closing the database.
- **GC safety:** Garbage collection skips S3 objects younger than 10 minutes to avoid race with in-flight async uploads.
- **Single instance:** `main` takes an exclusive `flock` on `<dbPath>.lock` before mounting or running a mutating subcommand (`gc`/`compact`/`migrate`/`purge-s3`/`restore`), so two processes cannot delete S3 objects out from under each other. `fsck`/`cache`/`version` return before the lock is taken.
- **`purge-s3` is the one irreversible command:** it deletes every object in the bucket and the local DB. It requires the bucket name typed back at a prompt (`--yes` skips it, `--dry-run` previews), and aborts on non-interactive stdin. Keep that guard: test and production buckets are commonly named alike and differ only by `--endpoint`, so an unguarded purge aimed at the wrong endpoint is total, silent data loss.
- **Volume accounting is self-derived:** `CommitInode` and `DeleteInodeWithVolume` re-read the inode's `vol_s3_key`/`vol_size` *inside* their own transaction rather than trusting a caller-supplied snapshot, and decrement the volume in that same transaction. This is what keeps a crash from leaving a still-referenced volume at `live_count=0` for GC to delete, and prevents a double decrement when a concurrent overwrite already moved the needle. Do not reintroduce a separate "mark dead then commit" step.
- **Staging claims:** The volume builder claims a staging file by renaming it to `<id>.packing` (or `.flushing`). Restoring a claim always goes through `restoreClaim`, which drops the stale claim instead of renaming it back when a concurrent overwrite has already staged newer data at the original path — a bare `os.Rename` there is a silent lost write.
- **Ownership normalization** of legacy `uid=0/gid=0` inodes runs **once**, guarded by the `default_ownership_normalized` key in the config table. Running it every boot would re-clobber files legitimately chown'd to root.
- **Download limit:** S3 downloads are capped at 2 GB to prevent OOM from corrupted or malicious objects.

## Known Limitations

- **Async Flush:** `Flush` (triggered by `close()`) launches the S3 upload asynchronously and returns success immediately. This means `close()` does **not** guarantee data durability. To ensure data is persisted to S3, call `fsync()` before `close()`. Standard tools like `cp` do not call `fsync`, so a failed upload after `cp` will only be logged, not reported to the user. `Fsync` waits for the upload and propagates errors.
- **No hard links:** S3 has no concept of hard links. `link()` returns `ENOTSUP`. Use symlinks instead.
- **No range reads or streaming under encryption:** A stored object is a single whole-file AES-256-GCM blob, so a byte-range slice is undecryptable ciphertext. Streaming mode is therefore disabled when an encryptor is configured, and encrypted media falls back to full download + decrypt. Restoring range reads for encrypted files needs chunked/segmented encryption (per-chunk nonces) — a format change.
- **`renameat2` flags:** `RENAME_EXCHANGE` is refused with `EINVAL` (an atomic two-inode swap is not implemented in the DB layer). It must stay `EINVAL` and **never** `ENOSYS`: on `ENOSYS` the kernel latches `fc->no_rename2` for the whole connection and answers every later `renameat2`-with-flags itself, which silently breaks `RENAME_NOREPLACE` mount-wide. `RENAME_NOREPLACE` is honored and returns `EEXIST`. Other flags fall through to the normal replace path.
- **Reader coherence:** A read-only handle that opened *before* a writer keeps reading its own snapshot of the file; it does not see the writer's changes until it reopens. Handles opened while a writer already has the file open do share state and see the writes. (Streaming/chunked reads deliberately bypass the shared buffer — see `--stream-rate`.)
- **No atomicity for large concurrent appends:** the kernel splits a write over `max_write` (128 KB) into several FUSE writes, and two appenders can interleave between them. Log lines and other small appends are unaffected.
