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
./hamstor gc            # Garbage collect orphaned S3 objects
./hamstor restore       # Restore DB from S3 via Litestream
```

Build embeds S3 credentials and passphrase via ldflags from `.env` (see `.env.example`).

Tests requiring S3 (upload, download) need credentials from `.env.test` (`source .env.test` before `go test`). Without it, S3-dependent tests skip gracefully.

## Architecture

**Data flow:** Write → memory buffer or spill file → async S3 upload on Flush → SQLite metadata commit → old S3 key deleted if update.

**Read path:** Check disk cache → S3 range request (unencrypted) → full S3 download + decrypt → cache on disk.

**Crash safety:** Files start as `status='pending'` in SQLite. On successful S3 upload they become `'committed'`. On startup, pending entries are cleaned up. GC removes orphaned S3 objects.

```
cmd/hamstor/main.go        Entry point, wires everything together
internal/hfuse/            FUSE implementation (go-fuse v2 fs.Node interface)
  fs.go                    HamstorFS - filesystem root, holds DB/S3/crypto/cache refs
  node.go                  HamstorNode - inode ops (Lookup, Readdir, Mkdir, Create, Unlink, Rename, Setattr, Symlink, Xattr)
  handle.go                HamstorHandle - file handle (Read/Write/Flush with cache and spill-to-disk)
  cleanup.go               Crash recovery on startup
internal/cache/            Disk cache with LRU eviction, keyed by S3 key
internal/db/               SQLite metadata (inodes + xattrs + config tables)
internal/s3store/          S3 client wrapper with retry logic; supports range requests
internal/crypto/           Optional AES-256-GCM encryption with Argon2id key derivation
internal/replicate/        Litestream integration for DB replication to S3
internal/ops/              GC (orphan cleanup) and S3 key migration
internal/thumb/            Freedesktop thumbnail generation for images
internal/creds/            Build-time embedded credentials via ldflags
```

## Key Patterns

- **S3 keys** are prefixed: `{first-2-hex}/{uuid}` (e.g., `a1/a1b2c3d4-...`). Legacy unprefixed keys can be migrated via `--migrate`.
- **Encryption** is optional (passphrase from flag > env > embedded). Format: `[version byte][12-byte nonce][ciphertext+tag]`. `IsEncrypted()` checks the version byte.
- **Async uploads:** `Flush` launches a goroutine for S3 upload, updates DB on completion. This enables parallel file copies.
- **Disk cache:** Downloaded/decrypted files are cached on local disk (`--cache-dir`). Keyed by S3 key (changes per version = automatic invalidation). LRU eviction with `--cache-size` limit.
- **Range requests:** For unencrypted files not yet in cache, reads use S3 Range headers instead of downloading the entire file. A background goroutine warms the cache.
- **Spill to disk:** Writes larger than 64 MB use a temp file instead of in-memory buffer, avoiding OOM on large files.
- **S3 retries:** All S3 operations retry up to 3 times with exponential backoff on transient failures.
- **UID/GID:** Stored in DB, set to caller's identity on creation, modifiable via chown.
- **Symlinks:** Stored as inodes with `symlink_target` column, mode `S_IFLNK`.
- **Extended attributes:** Stored in `xattrs` table (inode_id, name, value).
- **FUSE error mapping:** `sql.ErrNoRows` → `ENOENT`, other errors → `EIO`.
- **SQLite uses modernc.org/sqlite** (pure Go, no CGO). WAL mode, busy timeout 5s.
- **Litestream** runs embedded (not as a separate process), replicating to `litestream/` prefix in the same S3 bucket.
