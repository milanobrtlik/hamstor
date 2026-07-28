package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/user"
	"path/filepath"
	"strconv"

	"github.com/milan/hamstor/internal/crypto"
	"github.com/milan/hamstor/internal/db"
	"github.com/milan/hamstor/internal/ops"
	"github.com/milan/hamstor/internal/s3store"
	"github.com/milan/hamstor/internal/thumb"
)

// runThumbsCmd handles `hamstor thumbs sync`.
//
// It reads the database and the bucket and writes only to the freedesktop cache
// directory, which is why it can run against a live mount — see readOnlySubcmd
// for the two exemptions that makes possible. The mount-time pass covers the
// normal case; this exists for the one that matters most, a machine that has
// just restored the database and wants its thumbnails now rather than at the
// next restart.
func runThumbsCmd(ctx context.Context, database *db.DB, store *s3store.Store, enc *crypto.Encryptor, cache thumb.Cache, mountpoint string, args []string) {
	action := "sync"
	if len(args) > 0 {
		action = args[0]
	}
	if action != "sync" {
		log.Fatalf("thumbs: unknown action %q (want: sync)", action)
	}

	// The mountpoint is half the freedesktop cache key — the URI a thumbnail is
	// filed under is file://<mountpoint>/<path> — so without it this would write
	// a directory full of thumbnails nothing will ever look up.
	if mountpoint == "" {
		log.Fatalf("thumbs: --mount is required (it is part of the thumbnail's cache key)")
	}
	if cache.Dir == "" {
		log.Fatalf("thumbs: no thumbnail cache directory; pass --thumbnail-dir")
	}

	stats, err := ops.SyncThumbnails(ctx, database, store, enc, cache, mountpoint)
	if err != nil {
		log.Fatalf("thumbs: %v", err)
	}
	fmt.Printf("thumbs: %s -> %s\n", stats, cache.Dir)
}

// resolveThumbCache decides which freedesktop thumbnail directory to write into.
//
// This exists because os.UserCacheDir() is the wrong question. It reads
// $XDG_CACHE_HOME or $HOME from the DAEMON's environment, and systemd sets
// $HOME only for units that have User= set (systemd.exec(5)) — the hamstor unit
// runs as root without one, so both were unset, os.UserCacheDir() returned an
// error, and every thumbnail this filesystem ever generated went nowhere at all.
// Not to /root: nowhere. It failed that way from the day the feature landed.
//
// The right question is whose cache it is, and --uid already answers that: those
// are the files the mount serves. So:
//
//  1. an explicit --thumbnail-dir wins outright — it is also the only way to
//     name a target user's non-default $XDG_CACHE_HOME, which no amount of
//     guessing from this process can discover;
//  2. running as the target user ourselves, keep os.UserCacheDir(), which
//     honours that user's own $XDG_CACHE_HOME. This is the foreground case;
//  3. otherwise look the user up and use ~/.cache/thumbnails, chowning what we
//     write so they can still replace it.
//
// An empty Dir disables thumbnails, and the caller says so out loud at startup —
// silence about this is what let it stay broken for four months.
func resolveThumbCache(flagVal string, uid, gid int) thumb.Cache {
	if flagVal != "" {
		return thumb.Cache{Dir: flagVal, Uid: uid, Gid: gid}
	}

	// -1 leaves ownership alone, which is right when the files are already ours.
	if uid == os.Getuid() {
		if dir, err := os.UserCacheDir(); err == nil {
			return thumb.Cache{Dir: filepath.Join(dir, "thumbnails"), Uid: -1, Gid: -1}
		}
	}

	u, err := user.LookupId(strconv.Itoa(uid))
	if err != nil {
		log.Printf("hamstor: thumbnails: cannot resolve uid %d: %v", uid, err)
		return thumb.Cache{}
	}
	if u.HomeDir == "" {
		log.Printf("hamstor: thumbnails: uid %d has no home directory", uid)
		return thumb.Cache{}
	}
	return thumb.Cache{Dir: filepath.Join(u.HomeDir, ".cache", "thumbnails"), Uid: uid, Gid: gid}
}
