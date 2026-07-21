# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

Hamstor is a FUSE filesystem daemon (Go 1.25+) that presents S3-compatible object storage as a local mountable filesystem. Files are buffered locally (memory for small files, temp files for large), uploaded to S3 on flush, with metadata stored in SQLite. Litestream replicates the database to S3 for recovery.

**A storage-format change is half-landed:** large files move from one object per file to fixed 8 MiB blocks, which is what retires the read-modify-write on every write and the 2 GB ceiling below. The decisions, the call-site inventory (including the ten ways to lose data by missing one) and the implementation order are settled in `claudedocs/block-layout-design.md`. Read it before touching the write path or anything that assumes `s3_key` names a whole file — do not re-derive it.

Steps 0–3 are done: **the write path now produces blocks** (`blocks` table, `db.CommitBlocks`, one object per 8 MiB, encrypted per block) and `inodes.s3_key` is no longer written. Reading is deliberately still whole-file — `ensureLoaded` fetches every block and glues them into one buffer — so nothing is faster yet; that arrives with lazy materialization in step 5. Steps 4–7 remain: delete `inodes.s3_key`, lazy per-block faulting, retire the chunk sub-cache, and grow failed-upload retention to a block set.

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

**Every line of `.env.test` must start with `export`** (see `.env.test.example`). `testutil.RequireS3` reads the values with `os.Getenv`, so a plain `KEY=value` file sets shell variables that `go test` never sees: every S3 test skips and the run still prints `ok`. A green suite is therefore not by itself evidence the S3 tests ran — check for `SKIP`, or that the package took seconds rather than milliseconds. This is why `.env` is different: it is `include`d by the Makefile, not sourced, and must stay unprefixed.

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
- **Large files are stored as 8 MiB blocks** (`db.BlockSize`), one S3 object per block, indexed by `(inode_id, block_index)` in the `blocks` table. Five rules, and each of them is a way to lose data quietly:
  - **A flush uploads only the dirty blocks — unless the file is converting to blocks from something else.** `CommitBlocks` drops the previous storage *wholesale* (nulls `s3_key`, decrements the volume, clears `vol_*`), so a legacy whole-file object, a needle, or a staged file must be rewritten in full on the flush that converts it. Committing only the touched blocks leaves the rest of the file with no storage at all: change one byte of a 100 MB legacy file and 92 MB of it becomes holes. A brand-new file is *not* converting — forcing it to write every block would turn a sparse write at offset 1 GB into 128 objects of zeroes.
  - **Once an inode has blocks it keeps them,** even when a later write shrinks it below `MaxNeedleSize`. Falling back to volume staging commits through `CommitInode`, which knows nothing about blocks: the rows would survive and the read path (which checks blocks first) would serve the pre-shrink version.
  - **The keys a commit orphans are read inside its own transaction** and deleted only after it returns. `orphaned` is *not* "the previous key set" — an untouched block keeps its key, and deleting those deletes the live data of a file that was just written correctly.
  - **A block with no row is a hole** (reads as zeroes, never fetched); a block whose object is shorter than its live extent is a truncate that did not rewrite the tail. Neither is an error, and `inodes.size` — never `SUM(blocks.size)` — is the file's length.
  - **Every block is encrypted separately.** `crypto.Encrypt` emits a fresh `[version][nonce][ct+tag]` per call, so per-block encryption comes free and is what makes each object independently decryptable. Unencrypted blocks upload straight off the snapshot via `io.NewSectionReader`, which keeps both the heap (32 concurrent uploads against a 150 MB limit) and the SDK's no-alloc path intact.
- **A block-stored file has neither `s3_key` nor `vol_s3_key`** — which is exactly the shape that used to mean "staged, not yet packed" or "committed but has no data in S3". Four predicates therefore test for blocks too, and getting any of them wrong is silent: `ensureLoaded`/`Open` (else a healthy file returns `EIO` from the staging retry loop), `readStaged`, `Fsync` (else fsync on a large file returns `EIO`), and the shared `db.noBlockRows` clause in `GetStagedInodes`/`Fsck`/`CommitNeedlesToVolume`. The last one is load-bearing twice over: without it every healthy large file is reported unreadable at boot, *and* the volume builder may pack a forgotten staging file over an inode whose real data is in blocks.
- **Async uploads:** `Flush` launches a goroutine for S3 upload, updates DB on completion. This enables parallel file copies. Because a file's size only lands at `CommitInode`, the async path must invalidate the kernel's cached attributes afterwards (`inode.NotifyContent(-1, 0)` — a negative offset means attributes only, leaving the page cache alone). Without it `ls -l` reports 0 bytes for up to `--entry-timeout` (60s) after a `cp`.
- **Shared per-inode write state:** every open handle on one inode shares an `inodeWrite` (`internal/hfuse/writestate.go`), refcounted in `HamstorFS.writeStates`. Giving each handle its own buffer is what let two concurrent writers erase each other — each snapshotted the whole file and the last `Flush` wrote its snapshot back wholesale. Four rules hold this together:
  - **`writeMu` is a leaf lock.** It guards the map and the refcounts, nothing else. Never take `inodeWrite.mu` — or make a syscall, S3 or DB call — while holding it. `acquireWrite`/`tryAcquireWrite` return with it released, which is what lets `Flush` bump `uploadRefs` while already holding `st.mu` without inverting the order. A `truncate` racing a `close` would otherwise wedge the whole mount.
  - **The upload goroutine never takes `st.mu`.** That is what makes it safe for `Flush` and `ensureLoaded` to release the lock and wait on an attempt. It communicates via an immutable `uploadAttempt` (`err` written only before `close(done)`); do not go back to recycling shared `uploadDone`/`uploadErr` fields, which is a race the moment they are shared.
  - **The attempt is published before `Flush` first releases `st.mu`,** and `oldKey` is read only *after* the wait for a previous attempt. Publishing late reopens a seconds-wide window where a concurrent load takes the about-to-be-replaced key as its base; reading `oldKey` early makes the losing flush delete the winner's live object.
  - **A failed attempt poisons the state** (every later op returns `EIO`) until the last ref goes. The retained bytes under `<db-dir>/pending/` are then the only copy, and a sibling handle that quietly committed over them would flip the inode to `'committed'` — at which point `RecoverPending` deletes them as stale. See "Failed-upload recovery".
  Both the async and the volume-staging paths publish an attempt; staging returns early, so without one it races the async path outright.
- **`O_APPEND` is honored by the server, not the kernel:** `Write` ignores the offset it is handed for append-mode handles and appends at the true shared end of file. The kernel derives append offsets from its cached `st_size` (attr timeout, 60s), so two appenders get the same stale offset and one overwrites the other even with a shared buffer. Linux already ignores the offset for `pwrite()` on an `O_APPEND` fd, so this matches it.
- **`inodeWrite.size` is the only authority on how long the file is.** `HamstorHandle.fileSize` is an open-time snapshot that pairs with `s3Key` for `readChunked`/`readStreaming`, and a handle that never loads the state itself (a sibling got there first) keeps it forever — clamping `readLoaded` to it cuts everyone's contents down to whatever the file was when that handle opened. That is a silently lost line per race, and it is why the clamp uses `st.size`. Do not merge the two.
- **Reading a staged file must retry (`readStaged`).** The builder claims a staging file by renaming it to `<id>.packing` (Fsync: `.flushing`), packs it, then removes the claim — while a concurrent overwrite `Flush` writes a whole new file back at the original path. Every one of those states is transient, so one look that misses proves nothing; giving up returns `EIO` for data sitting on disk. Below `TargetVolumeSize` the builder claims and restores *every* staged file on *every* notify, so an append-per-line workload runs this constantly. `Open`'s write preload shares `readStaged` with `ensureLoaded` — a second copy gets the retry wrong differently.
- **`Open`'s write preload settles the inode under `st.mu`:** it waits out any in-flight upload and re-reads the metadata before loading. The preload both loads from storage *and* clamps the shared buffer to `meta.Size`, so a `meta` read before the state was locked lets a sibling's flush land in between — the clamp then cuts their write off the end and the next append overwrites the remains. Read-only opens deliberately skip the wait so they never block behind an upload.
- **Thumbnails** are generated from the plaintext *on disk* (the spill file, or a temp copy for staged small files), never from a heap buffer, and a bounded worker pool reads each source only after taking a slot. Holding the image in a queued goroutine instead puts one full-size buffer per pending file in RAM. Generation is gated on that path existing — do not re-gate it on an in-memory buffer, which is how it silently broke for unencrypted mounts once uploads started streaming from disk.
- **Disk cache:** Downloaded/decrypted files are cached on local disk (`--cache-dir`). Keyed by S3 key (changes per version = automatic invalidation). LRU eviction with `--cache-size` limit.
- **Packed volumes are cached at the write, not the read.** Whole-volume reads look for `volobj/{volKey}` (`readNeedle`), and the volume builder already holds those exact bytes in memory when it seals — so it stores them itself (`cacheVolume`, after a successful `CommitNeedlesToVolume` only). Without that put, a bulk write of small files sends its data on a local disk → S3 → local disk round trip: every volume is downloaded back seconds after it was uploaded, just to be readable. The bytes are byte-identical either way — needles are encrypted individually *before* staging, so the volume object itself is never transformed. This is safe where per-file caching after `Flush` was not (`handle.go`): one put per 8 MB volume, not one per file, so it does not churn the cache during a bulk copy.
- **Standalone objects are cached at the write too** (`cacheUploaded`, the counterpart of `cacheVolume` above). After `CommitInode` succeeds, `flushAsync` copies the spill file into the cache under the new key, so reopening a file for writing reads it locally instead of downloading back the bytes that were on this disk seconds ago. Three rules:
  - **Cache the spill file's plaintext, never `uploadData`.** Unlike needles, a standalone object is encrypted *whole* at upload time, and a cache entry is served straight back as file contents (`ensureLoaded` decrypts only what came from `Store.Download`). Caching the ciphertext hands encrypted bytes to every reader.
  - Only after `committed == true`. The other post-upload paths delete the object they just wrote, so caching there would serve bytes nothing references.
  - The spill file therefore outlives the upload on every path. On failure it is still what `retainPendingUpload` retains — but only when nothing encrypted it, since recovery re-uploads the retained bytes verbatim. Ownership then passes to `scheduleThumb`, which removes non-images itself.
  A file larger than `1/maxCacheShare` of `--cache-size` is skipped: it would evict most of the cache on the way in and, being the biggest entry, buy little for what it displaced.
- **Range requests:** For unencrypted files not yet in cache, reads use S3 Range headers instead of downloading the entire file. A background goroutine warms the cache. `Read` takes this path only when the whole file is *not* cached: range-reading an object we already hold locally would re-download it, and `PutChunk` deletes the whole-file entry to put its chunk directory at that path — which would throw away the copy the flush just kept. For the same reason `cache.Open`/`Has` report a chunk directory as absent (`os.Open` succeeds on a directory, and the write preload in `Open` discards the resulting `EISDIR`, so it would take a few KB of zeros for the file's contents).
- **Spill to disk:** Writes larger than 64 MB use a temp file instead of in-memory buffer, avoiding OOM on large files.
- **S3 retries:** All S3 operations retry up to 3 times with exponential backoff on transient failures.
- **Response checksums are deliberately not validated** (`ResponseChecksumValidationWhenRequired`, set in `s3store.New`). The SDK default validates whenever the server reports a checksum, but for a multipart-uploaded object that checksum covers the concatenated *part* checksums rather than the object's bytes, so a whole-object GET can never satisfy it. Whether that bites depends on the backend, which is why it read as flakiness: B2 appends the part count and the SDK skips validation, Garage reports a bare value and `Download` burns all three retries on `checksum did not match` for bytes that are perfectly fine. Do not restore the default. It makes every object above the uploader's part size undownloadable as a whole — unopenable for writing, and unreadable outright when encrypted, since encryption disables the range path. The cost is no CRC check on single-PUT GETs either; under encryption AES-256-GCM is authenticated so nothing is lost, and unencrypted mounts fall back to TLS and the provider's own integrity.
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
- **Staging claims:** The volume builder claims a staging file by renaming it to `<id>.packing` (or `.flushing`). Restoring a claim always goes through `restoreClaim`, which drops the stale claim instead of renaming it back when a concurrent overwrite has already staged newer data at the original path — a bare `os.Rename` there is a silent lost write. The check and the rename are **one** `renameat2(RENAME_NOREPLACE)` call: a stat followed by a rename loses the race often enough to cost real data (the stale claim lands back over the newer file while the DB advertises the newer size, and the next open reads short and appends over it). The mtime CAS in `CommitNeedlesToVolume` is *not* a backstop for that — it guards the volume commit, not the staging file.
- **Ownership normalization** of legacy `uid=0/gid=0` inodes runs **once**, guarded by the `default_ownership_normalized` key in the config table. Running it every boot would re-clobber files legitimately chown'd to root.
- **Download limit:** S3 downloads are capped at 2 GB to prevent OOM from corrupted or malicious objects. It now applies per *object*, so for a block-stored file it is a sanity check on one 8 MiB block and the old consequence is gone — such a file reopens for writing at any size. It still bites **legacy whole-file objects** written before the block layout: those go through `Store.Download` in one piece, so above 2 GB they cannot be reopened for writing and, under encryption, cannot be read at all once out of cache. Rewriting such a file converts it to blocks and fixes it permanently.

## Known Limitations

- **Async Flush:** `Flush` (triggered by `close()`) launches the S3 upload asynchronously and returns success immediately. This means `close()` does **not** guarantee data durability. To ensure data is persisted to S3, call `fsync()` before `close()`. Standard tools like `cp` do not call `fsync`, so a failed upload after `cp` will only be logged, not reported to the user. `Fsync` waits for the upload and propagates errors.
- **No hard links:** `link()` returns `ENOTSUP`. Not because of S3 — a hard link is purely a metadata concept the object store has no say in, and S3QL supports them over the same kind of backend. The blocker is the schema: `inodes` conflates the dentry with the inode, keeping `parent_id` and `name` in the inode row under `UNIQUE(parent_id, name)`, so one inode has exactly one name. Supporting hard links needs a separate `dentries` table — schema surgery worth doing only alongside another format migration, not on its own. Use symlinks instead.
- **No range reads or streaming under encryption:** A stored object is a single whole-file AES-256-GCM blob, so a byte-range slice is undecryptable ciphertext. Streaming mode is therefore disabled when an encryptor is configured, and encrypted media falls back to full download + decrypt. Restoring range reads for encrypted files needs chunked/segmented encryption (per-chunk nonces) — a format change.
- **`renameat2` flags:** `RENAME_EXCHANGE` is refused with `EINVAL` (an atomic two-inode swap is not implemented in the DB layer). It must stay `EINVAL` and **never** `ENOSYS`: on `ENOSYS` the kernel latches `fc->no_rename2` for the whole connection and answers every later `renameat2`-with-flags itself, which silently breaks `RENAME_NOREPLACE` mount-wide. `RENAME_NOREPLACE` is honored and returns `EEXIST`. Other flags fall through to the normal replace path.
- **Reader coherence:** A read-only handle that opened *before* a writer keeps reading its own snapshot of the file; it does not see the writer's changes until it reopens. Handles opened while a writer already has the file open do share state and see the writes. (Streaming/chunked reads deliberately bypass the shared buffer — see `--stream-rate`.)
- **No atomicity for large concurrent appends:** the kernel splits a write over `max_write` (128 KB) into several FUSE writes, and two appenders can interleave between them. Log lines and other small appends are unaffected.
