package main

import (
	"log"
	"os"
	"os/user"
	"path/filepath"
	"strconv"

	"github.com/milan/hamstor/internal/thumb"
)

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
