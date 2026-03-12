# Hamstor

FUSE filesystem that stores files in S3-compatible object storage with SQLite metadata. Includes automatic database replication via Litestream and crash recovery.

## How it works

- Files are buffered in memory and uploaded to S3 on flush
- Metadata (directory tree, permissions, timestamps) lives in a local SQLite database
- Litestream continuously replicates the database to S3 and can restore it on startup
- Incomplete uploads from crashes are cleaned up automatically
- Thumbnails are generated for image files (freedesktop spec)

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

Credentials are embedded into the binary at build time via ldflags.

## Install

```sh
make install
```

This builds the binary, copies it to `/usr/local/bin`, creates a systemd service, and starts it. The filesystem will be mounted at `HAMSTOR_MOUNT`.

## Uninstall

```sh
make uninstall
```

Stops the service, unmounts the filesystem, and removes the binary, service file, and database directory (`/var/lib/hamstor`).

## Manual usage

```sh
make build
./hamstor --mount /path/to/mount --bucket my-bucket --endpoint https://s3.example.com
```

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--mount` | _(required)_ | Mount point |
| `--bucket` | _(required)_ | S3 bucket name |
| `--endpoint` | | S3 endpoint URL |
| `--region` | from `.env` | S3 region |
| `--db` | `data/hamstor.db` | SQLite database path |
| `--replicate` | `true` | Enable Litestream replication to S3 |