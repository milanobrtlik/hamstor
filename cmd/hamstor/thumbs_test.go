package main

import (
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"testing"
)

// TestResolveThumbCacheExplicitDirWins: --thumbnail-dir is the only way to name
// a target user's non-default XDG_CACHE_HOME, so nothing may override it.
func TestResolveThumbCacheExplicitDirWins(t *testing.T) {
	t.Setenv("XDG_CACHE_HOME", "/should/be/ignored")
	c := resolveThumbCache("/explicit/thumbs", 4242, 4243)
	if c.Dir != "/explicit/thumbs" {
		t.Errorf("Dir = %q, want /explicit/thumbs", c.Dir)
	}
	// An explicit dir for another uid still has to be chowned to them, or they
	// cannot replace what root wrote.
	if c.Uid != 4242 || c.Gid != 4243 {
		t.Errorf("ownership = %d:%d, want 4242:4243", c.Uid, c.Gid)
	}
}

// TestResolveThumbCacheSelfHonoursXDG covers the foreground case: running as
// the target user, that user's own XDG_CACHE_HOME is authoritative and no
// chown is needed or possible.
func TestResolveThumbCacheSelfHonoursXDG(t *testing.T) {
	home := t.TempDir()
	t.Setenv("XDG_CACHE_HOME", home)

	c := resolveThumbCache("", os.Getuid(), os.Getgid())
	want := filepath.Join(home, "thumbnails")
	if c.Dir != want {
		t.Errorf("Dir = %q, want %q", c.Dir, want)
	}
	if c.Uid != -1 || c.Gid != -1 {
		t.Errorf("ownership = %d:%d, want -1:-1 (the files are already ours)", c.Uid, c.Gid)
	}
}

// TestResolveThumbCacheOtherUserIgnoresOurEnv is the regression for the actual
// bug. The daemon runs as root with --uid pointing at the desktop user; our own
// environment says nothing about where THAT user's cache is, and under systemd
// it says nothing at all. The answer must come from the passwd entry.
func TestResolveThumbCacheOtherUserIgnoresOurEnv(t *testing.T) {
	other := 0
	if os.Getuid() == 0 {
		t.Skip("running as root: no 'other user' to resolve")
	}
	u, err := user.LookupId(strconv.Itoa(other))
	if err != nil || u.HomeDir == "" {
		t.Skipf("uid %d not resolvable here", other)
	}

	// Poison our environment in both the ways os.UserCacheDir() consults.
	t.Setenv("XDG_CACHE_HOME", "/our/cache/not/theirs")
	t.Setenv("HOME", "/our/home/not/theirs")

	c := resolveThumbCache("", other, other)
	want := filepath.Join(u.HomeDir, ".cache", "thumbnails")
	if c.Dir != want {
		t.Errorf("Dir = %q, want %q (our own env must not leak in)", c.Dir, want)
	}
	if c.Uid != other || c.Gid != other {
		t.Errorf("ownership = %d:%d, want %d:%d", c.Uid, c.Gid, other, other)
	}
}

// TestResolveThumbCacheUnresolvableDisables: an unknown uid yields an empty
// Cache, which is inert. It must not fall back to OUR cache directory — that is
// how thumbnails would land in root's home again.
func TestResolveThumbCacheUnresolvableDisables(t *testing.T) {
	c := resolveThumbCache("", 2147483600, 2147483600)
	if c.Dir != "" {
		t.Errorf("Dir = %q, want empty for an unresolvable uid", c.Dir)
	}
}
