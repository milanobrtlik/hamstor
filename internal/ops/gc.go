package ops

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/milan/hamstor/internal/db"
	"github.com/milan/hamstor/internal/s3store"
)

// gcGracePeriod is the minimum age of an S3 object before GC considers it
// orphaned. This prevents deleting objects from in-flight async uploads
// that haven't been committed to the DB yet.
const gcGracePeriod = 10 * time.Minute

// The guard: a GC run must not delete an implausible share of the bucket.
//
// Phrased for humans, gcMaxOrphanRatio says the database has to account for at
// least 75% of the objects it can classify. The numbers come from how orphans
// actually arise here, which is in bursts that are nevertheless a small share of
// a mature store:
//
//   - Uploads cancelled at shutdown. Every block an interrupted flush had
//     already PUT is an orphan forever, because recovery re-uploads under fresh
//     keys and never reuses what is there. Bounded by UploadSem (32) times the
//     blocks each flush got through, so one interrupted multi-gigabyte file can
//     leave ~10^3.
//   - Uploads that failed outright, and crashes: the same shape.
//   - A failed CommitNeedlesToVolume in compact: about one object.
//
// Against that, 1 TB of data is ~131k block objects, so even several incidents
// stay well under 1%. A quarter is far outside anything routine.
//
// gcMinOrphans is not there to separate legitimate from illegitimate — no
// absolute number can, since one bad shutdown legitimately orphans hundreds. Its
// only job is to stop the ratio meaning anything on a four-object bucket.
//
// Both are constants rather than flags on purpose: a threshold in a unit file is
// a threshold somebody turns off. The one escape hatch is --allow-mass-delete.
//
// What this cannot catch: a MILDLY stale database. A snapshot 5% behind reads as
// an ordinary orphan set and deletes 5% of live data, and no cheap signal
// distinguishes the two. Against staleness the defences are the existence check
// on the database and the flock, not this ratio.
const (
	gcMinOrphans     = 20
	gcMaxOrphanRatio = 0.25
)

type GCResult struct {
	OrphansFound   int
	OrphansDeleted int
	Errors         int
	DBOrphans      int
}

// GCConfig is the CLI's view of a run. Production always looks at the whole
// bucket with the full grace period, so the only things it gets to choose are
// these.
type GCConfig struct {
	DryRun          bool
	AllowMassDelete bool
	ExcludePrefixes []string
}

// gcCounts is what the guard judges: the shape of the run, before anything has
// been deleted. Only non-excluded objects are counted.
type gcCounts struct {
	Matched  int // key is still referenced by the database
	TooYoung int // unknown, but inside the grace period
	Orphans  int // unknown and past it — the phase 1 delete set
}

// GCGuardError reports a run refused because the database does not plausibly
// describe this bucket. Exported so the CLI can errors.As it and say so in the
// terms the operator needs; the counts are here rather than the bucket name
// because naming the bucket and the --db path is the caller's job.
type GCGuardError struct {
	Reason   string
	Matched  int
	TooYoung int
	Orphans  int
}

// Classified is the guard's denominator: the objects the run could place on one
// side or the other. An unknown object inside the grace period is evidence for
// neither — it is either an upload still in flight or the visible tip of a stale
// database — so it belongs in neither the numerator nor the denominator.
func (e *GCGuardError) Classified() int { return e.Matched + e.Orphans }

func (e *GCGuardError) Ratio() float64 {
	if e.Classified() == 0 {
		return 0
	}
	return float64(e.Orphans) / float64(e.Classified())
}

func (e *GCGuardError) Error() string {
	return fmt.Sprintf("gc refused: %s — the database references %d of the %d objects it can classify; "+
		"%d would be deleted, %d are inside the grace period",
		e.Reason, e.Matched, e.Classified(), e.Orphans, e.TooYoung)
}

// checkGCGuard is pure — no S3, no database, no clock — so the thresholds can be
// tested exhaustively without a bucket.
func checkGCGuard(c gcCounts) *GCGuardError {
	if c.Orphans == 0 {
		return nil
	}
	// A healthy filesystem always recognises something here. Zero overlap means
	// this database is not describing this bucket at all: it was created empty a
	// moment ago (the wrong --db), or it belongs to a different endpoint. Testing
	// the overlap rather than the size of the key set catches the second case
	// too, and costs one counter.
	if c.Matched == 0 {
		return &GCGuardError{
			Reason:   "the database references none of the objects in this bucket",
			Matched:  c.Matched,
			TooYoung: c.TooYoung,
			Orphans:  c.Orphans,
		}
	}
	if c.Orphans >= gcMinOrphans && float64(c.Orphans) > gcMaxOrphanRatio*float64(c.Matched+c.Orphans) {
		return &GCGuardError{
			Reason:   fmt.Sprintf("%.0f%% of this bucket is unaccounted for", 100*float64(c.Orphans)/float64(c.Matched+c.Orphans)),
			Matched:  c.Matched,
			TooYoung: c.TooYoung,
			Orphans:  c.Orphans,
		}
	}
	return nil
}

// gcOptions narrows what a GC run may look at. Production always uses the whole
// bucket and the full grace period; the phase 1 test sets both, because with the
// real grace period a freshly uploaded object is skipped before its key is ever
// compared against the database — so it would survive even a GC that has lost
// track of it, and the test would prove nothing. Scoping the listing then keeps
// that zero grace period from reaching the objects other packages' tests are
// using in the same bucket.
type gcOptions struct {
	grace      time.Duration
	listPrefix string
	// allowMassDelete disables the guard. The zero value keeps it ON, on
	// purpose: a caller that has never heard of this field is protected, not
	// exposed. Tests do not reach for it — they scope listPrefix instead, so
	// they only ever see objects they created and the counts are deterministic.
	allowMassDelete bool
}

func GC(ctx context.Context, database *db.DB, store *s3store.Store, cfg GCConfig) (*GCResult, error) {
	return gcScoped(ctx, database, store, cfg.DryRun,
		gcOptions{grace: gcGracePeriod, allowMassDelete: cfg.AllowMassDelete},
		cfg.ExcludePrefixes...)
}

func gcScoped(ctx context.Context, database *db.DB, store *s3store.Store, dryRun bool, opts gcOptions, excludePrefixes ...string) (*GCResult, error) {
	knownKeys, err := database.AllS3KeySet()
	if err != nil {
		return nil, fmt.Errorf("gc: load keys from db: %w", err)
	}
	log.Printf("gc: %d keys in database", len(knownKeys))

	s3Objects, err := store.ListObjects(ctx, opts.listPrefix)
	if err != nil {
		return nil, fmt.Errorf("gc: list s3 objects: %w", err)
	}
	log.Printf("gc: %d objects in S3", len(s3Objects))

	// Default exclude prefixes
	if len(excludePrefixes) == 0 {
		excludePrefixes = []string{"litestream/"}
	}

	cutoff := time.Now().Add(-opts.grace)
	result := &GCResult{}
	counts := gcCounts{}
	var orphanKeys []string
	for _, obj := range s3Objects {
		excluded := false
		for _, prefix := range excludePrefixes {
			if strings.HasPrefix(obj.Key, prefix) {
				excluded = true
				break
			}
		}
		if excluded {
			continue
		}
		if _, ok := knownKeys[obj.Key]; ok {
			counts.Matched++
			continue
		}
		// Skip recently created objects — they may be from in-flight async uploads
		if !obj.LastModified.IsZero() && obj.LastModified.After(cutoff) {
			counts.TooYoung++
			continue
		}

		counts.Orphans++
		result.OrphansFound++
		if dryRun {
			log.Printf("gc: orphan (dry-run): %s", obj.Key)
			continue
		}
		orphanKeys = append(orphanKeys, obj.Key)
	}

	// Log the guard's arithmetic on every run, not only when it fires, so the
	// number is observable long before the day it matters.
	if classified := counts.Matched + counts.Orphans; classified > 0 {
		log.Printf("gc: database accounts for %d/%d classified objects (%.1f%%)",
			counts.Matched, classified, 100*float64(counts.Matched)/float64(classified))
	}

	// Phase 2's query is read here, above the first deletion, so that a refusal
	// below is a run in which nothing at all was deleted.
	orphanedInodes, err := database.GetOrphanedInodes()
	if err != nil {
		return nil, fmt.Errorf("gc: find orphaned inodes: %w", err)
	}

	// The guard's claim is that this database is not a trustworthy description of
	// this filesystem. Phases 2 and 3 are entirely database-driven, and phase 2 is
	// the only one that destroys metadata rather than merely objects — so a
	// refusal aborts the whole run rather than only phase 1. A dry run mutates
	// nothing, so it finishes the report first and returns the refusal at the
	// end: the listing above is exactly what the operator needs in order to judge
	// it.
	guardErr := checkGCGuard(counts)
	if guardErr != nil && !opts.allowMassDelete && !dryRun {
		return result, guardErr
	}

	if len(orphanKeys) > 0 {
		deleted, err := store.DeleteBatch(ctx, orphanKeys)
		result.OrphansDeleted += deleted
		if err != nil {
			log.Printf("gc: batch delete: %v", err)
			result.Errors += len(orphanKeys) - deleted
		}
	}

	// Phase 2: DB inodes whose parent no longer exists (queried above)

	for _, meta := range orphanedInodes {
		result.DBOrphans++
		if dryRun {
			log.Printf("gc: db orphan (dry-run): inode %d %q vol_s3_key=%s", meta.ID, meta.Name, meta.VolS3Key)
		}
	}

	// Delete DB inode rows (atomically with volume stats update), then the block
	// objects they owned.
	//
	// The keys are collected FROM the delete, not before it. An orphaned inode's
	// blocks are removed by ON DELETE CASCADE, so after this call nobody knows
	// them any more — collecting them from the inode row, the way this used to,
	// found nothing at all and left every block of every orphaned file in the
	// bucket forever. Phase 1 would eventually sweep them, but only because it
	// now knows about blocks: the two omissions used to mask each other.
	if !dryRun {
		var dbOrphanS3Keys []string
		for _, meta := range orphanedInodes {
			orphaned, err := database.DeleteInodeWithVolume(meta.ID, meta.VolS3Key)
			if err != nil {
				log.Printf("gc: delete orphan inode %d: %v", meta.ID, err)
				result.Errors++
				continue
			}
			dbOrphanS3Keys = append(dbOrphanS3Keys, orphaned...)
			result.OrphansDeleted++
		}
		if len(dbOrphanS3Keys) > 0 {
			if _, err := store.DeleteBatch(ctx, dbOrphanS3Keys); err != nil {
				log.Printf("gc: batch delete db orphan block keys: %v", err)
				result.Errors += len(dbOrphanS3Keys)
			}
		}
	}

	// Phase 3: delete empty volumes (all needles deleted, older than grace period)
	graceNs := int64(opts.grace / time.Nanosecond)
	emptyVols, err := database.GetEmptyVolumes(graceNs)
	if err != nil {
		log.Printf("gc: get empty volumes: %v", err)
	}
	for _, vol := range emptyVols {
		result.OrphansFound++
		if dryRun {
			log.Printf("gc: empty volume (dry-run): %s", vol.S3Key)
			continue
		}
		if err := store.Delete(ctx, vol.S3Key); err != nil {
			log.Printf("gc: delete empty volume %s: %v", vol.S3Key, err)
			result.Errors++
			continue
		}
		if err := database.DeleteVolume(vol.S3Key); err != nil {
			log.Printf("gc: delete empty volume row %s: %v", vol.S3Key, err)
			result.Errors++
			continue
		}
		result.OrphansDeleted++
		log.Printf("gc: deleted empty volume: %s", vol.S3Key)
	}

	// Only a dry run reaches here with a refusal outstanding; a real one returned
	// above, before deleting anything.
	if guardErr != nil && !opts.allowMassDelete {
		return result, guardErr
	}
	return result, nil
}
