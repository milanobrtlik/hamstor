# Hamstor

FUSE filesystem that stores files in S3-compatible object storage with SQLite metadata. Includes automatic database replication via Litestream and crash recovery.

## How it works

- Files are buffered in memory and uploaded to S3 on flush
- Metadata (directory tree, permissions, timestamps) lives in a local SQLite database
- Litestream continuously replicates the database to S3 and can restore it on startup
- Incomplete uploads from crashes are cleaned up automatically
- Thumbnails are generated for image files (freedesktop spec)
- File contents can optionally be encrypted at rest (see [Encryption](#encryption))

## Requirements

- Go 1.25+
- FUSE (`fusermount` available on the system)
- S3-compatible storage (Backblaze B2, MinIO, Garage, AWS S3, ...)

## Configuration

Copy `.env.example` to `.env` and fill in your values:

```sh
cp .env.example .env
```

| Variable | Description |
|----------|-------------|
| `AWS_ACCESS_KEY_ID` | S3 access key |
| `AWS_SECRET_ACCESS_KEY` | S3 secret key |
| `AWS_REGION` | S3 region (e.g. `eu-central-003`) |
| `HAMSTOR_BUCKET` | S3 bucket name |
| `HAMSTOR_ENDPOINT` | S3 endpoint URL |
| `HAMSTOR_MOUNT` | Local mount point path |
| `HAMSTOR_PASSPHRASE` | Encryption passphrase — leave empty for an unencrypted mount (see [Encryption](#encryption)) |

Credentials and the passphrase are embedded into the binary at build time via ldflags. `.env` is read by the Makefile, not by the running daemon — there is no `--access-key` flag, and changes to `.env` only take effect after a rebuild.

## Encryption

Encryption is **optional and off by default**. It is enabled solely by setting a passphrase — in `.env` before `make build`, via the `HAMSTOR_PASSPHRASE` environment variable, or the `--passphrase` flag. An empty passphrase everywhere means an unencrypted mount; there is no separate mode switch. Under systemd only the build-time embedded value applies, because the generated unit passes neither the flag nor the environment variable.

File **contents** are stored as whole-file AES-256-GCM blobs (`[version][12-byte nonce][ciphertext+tag]`), with the key derived via Argon2id. The random salt is generated on the first encrypted start and stored in the SQLite database.

What encryption does **not** protect — know this before relying on it:

- **Metadata is plaintext.** Filenames, the directory tree, sizes, timestamps, permissions, uid/gid, symlink targets and xattrs all live in the SQLite database in the clear — and Litestream replicates that database to the same bucket. Anyone with read access to the bucket sees every path and size; only the contents are opaque.
- **The local disk holds plaintext.** The disk cache, spill files (writes over 64 MB) and generated thumbnails are all written decrypted.
- **No range reads or streaming under encryption.** A stored object is one whole-file blob, so reads fall back to full download + decrypt.

Caveats:

- The salt lives in the database, not in the passphrase — the passphrase alone cannot recover data. `make uninstall` deletes the salt (see below).
- Changing the passphrase against an existing database silently derives a wrong key (the stored salt is reused and there is no verifier); failures surface later as read errors.

## Install

```sh
make install
```

This builds the binary, copies it to `/usr/local/bin`, creates a systemd service, and starts it. The filesystem will be mounted at `HAMSTOR_MOUNT`.

## Uninstall

```sh
make uninstall
```

Stops the service, unmounts the filesystem, and removes the binary, service file, and the entire data directory (`/var/lib/hamstor`) — database, disk cache, and (for encrypted mounts) the encryption salt.

**This is destructive.** For an encrypted mount, deleting the salt makes the data unrecoverable unless the database was replicated. Make sure Litestream replication has been running (it is on by default) before uninstalling.

## Manual usage

```sh
make build
./hamstor --mount /path/to/mount --bucket my-bucket --endpoint https://s3.example.com
```

When running as a normal user, pass `--cache-dir` to a writable path. The default (`/var/lib/hamstor/cache`) is not writable without root, and hamstor silently continues **without a cache** (which also disables range reads) when it cannot create it.

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--mount` | _(required)_ | Mount point |
| `--bucket` | _(required)_ | S3 bucket name (required for every subcommand except `version`, `fsck`, `cache`) |
| `--endpoint` | | S3 endpoint URL (Garage/MinIO); a non-empty value enables path-style addressing |
| `--region` | from `.env` | S3 region |
| `--db` | `data/hamstor.db` | SQLite database path; its directory also holds `spill/`, `staging/`, `pending/` and the lock file |
| `--replicate` | `true` | Enable Litestream replication to S3 |
| `--passphrase` | | Encryption passphrase (or `HAMSTOR_PASSPHRASE`) — enables encryption, see [Encryption](#encryption) |
| `--cache-dir` | `/var/lib/hamstor/cache` | Local disk cache directory |
| `--cache-size` | `10` | Max cache size in GB (`0` disables the cache and range reads) |
| `--uid` | caller's UID | Default file owner UID (needed under systemd, which runs as root) |
| `--gid` | caller's GID | Default file owner GID |
| `--stream-rate` | `5` | Streaming rate limit in MB/s for media (`0` disables streaming) |
| `--stream-buffer` | `16` | Streaming memory buffer in MB |
| `--entry-timeout` | `60s` | FUSE entry/attr cache timeout |
| `--volume-packing` | `true` | Pack files under 256 KB into shared volume objects |
| `--compact-ratio` | `0.5` | Dead-space ratio threshold for `compact` |
| `--dry-run` | `false` | Preview mode for `gc`, `compact` and `purge-s3` |
| `--yes` | `false` | Skip the `purge-s3` confirmation prompt |
| `--pprof` | | pprof listen address (e.g. `:6060`) |

### Subcommands

Flags work on either side of the subcommand — `hamstor gc --bucket x` and `hamstor --bucket x gc` are equivalent.

| Command | Description |
|---------|-------------|
| _(none)_ | Mount mode (restore, replicate, recover, then mount) |
| `version` | Print version and exit |
| `fsck` | Check filesystem consistency (needs only `--db`); exits non-zero on problems |
| `cache stats` | Show cache directory, entry count, size and limit |
| `cache clear` | Empty the disk cache (safe — data is re-fetched on demand) |
| `gc` | Delete orphaned S3 objects (skips objects younger than 10 minutes) |
| `compact` | Rewrite volumes whose dead space exceeds `--compact-ratio` |
| `migrate` | Migrate legacy unprefixed S3 keys to `{2-hex}/{uuid}` |
| `restore` | Restore the database from S3 via Litestream (only when no local DB exists) |
| `purge-s3` | **Destructive:** delete every object in the bucket and the local database |

`gc`, `compact`, `migrate`, `restore` and `purge-s3` take an exclusive lock on `<db>.lock`; `fsck`, `cache` and `version` do not.