# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

Hamstor is a FUSE filesystem daemon (Go 1.25+) that presents S3-compatible object storage as a local mountable filesystem. Files are buffered in memory, uploaded to S3 on flush, with metadata stored in SQLite. Litestream replicates the database to S3 for recovery.

## Commands

```bash
make build              # Build binary (requires .env with S3 credentials)
go test ./...           # Run all tests
go test ./internal/hfuse/  # Run tests for a single package
go vet ./...            # Lint
./hamstor --mount /path --bucket name --endpoint https://s3.example.com  # Run manually
```

Build embeds S3 credentials and passphrase via ldflags from `.env` (see `.env.example`).

## Architecture

**Data flow:** Write → memory buffer (`HamstorHandle.buf`) → async S3 upload on Flush → SQLite metadata commit → old S3 key deleted if update.

**Crash safety:** Files start as `status='pending'` in SQLite. On successful S3 upload they become `'committed'`. On startup, pending entries are cleaned up. GC removes orphaned S3 objects.

```
cmd/hamstor/main.go        Entry point, wires everything together
internal/hfuse/            FUSE implementation (go-fuse v2 fs.Node interface)
  fs.go                    HamstorFS - filesystem root, holds DB/S3/crypto refs
  node.go                  HamstorNode - inode ops (Lookup, Readdir, Mkdir, Create, Unlink, Rename, Setattr)
  handle.go                HamstorHandle - file handle (Read downloads from S3, Write buffers, Flush uploads)
  cleanup.go               Crash recovery on startup
internal/db/               SQLite metadata (inodes table + config table for encryption salt)
internal/s3store/          S3 client wrapper; key format is {first-2-hex}/{uuid}
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
- **FUSE error mapping:** `sql.ErrNoRows` → `ENOENT`, other errors → `EIO`.
- **SQLite uses modernc.org/sqlite** (pure Go, no CGO).
- **Litestream** runs embedded (not as a separate process), replicating to `litestream/` prefix in the same S3 bucket.