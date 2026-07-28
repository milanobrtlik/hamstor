package ops

import "testing"

// TestCheckGCGuard is where the two thresholds are documented. checkGCGuard is
// pure, so every case here costs microseconds and none of them needs a bucket —
// which matters, because the cases that must NOT trip are the ones a shared
// test bucket can never demonstrate reliably.
func TestCheckGCGuard(t *testing.T) {
	tests := []struct {
		name  string
		in    gcCounts
		trips bool
		why   string
	}{
		{
			name:  "nothing to delete",
			in:    gcCounts{Matched: 0, Orphans: 0, TooYoung: 3},
			trips: false,
			why:   "a bucket holding only litestream/ and in-flight uploads is not evidence of anything",
		},
		{
			name:  "empty database, populated bucket",
			in:    gcCounts{Matched: 0, Orphans: 1},
			trips: true,
			why:   "the reported bug: a single orphan is enough when the database recognises nothing",
		},
		{
			name:  "wrong endpoint: keys exist but none of them are here",
			in:    gcCounts{Matched: 0, Orphans: 5000},
			trips: true,
			why:   "zero overlap is the same signal whether the key set is empty or merely foreign",
		},
		{
			name:  "one orphan below the floor, everything else accounted for",
			in:    gcCounts{Matched: 1, Orphans: gcMinOrphans - 1},
			trips: false,
			why:   "the floor stops the ratio meaning anything on a tiny bucket",
		},
		{
			name:  "at the floor, almost nothing accounted for",
			in:    gcCounts{Matched: 1, Orphans: gcMinOrphans},
			trips: true,
			why:   "past the floor the ratio governs, and 20/21 is not a routine orphan set",
		},
		{
			name:  "exactly at the ratio",
			in:    gcCounts{Matched: 300, Orphans: 100},
			trips: false,
			why:   "the comparison is strict: 100/400 == gcMaxOrphanRatio must pass",
		},
		{
			name:  "just past the ratio",
			in:    gcCounts{Matched: 299, Orphans: 101},
			trips: true,
			why:   "101/400 is over a quarter",
		},
		{
			name:  "a bad shutdown on a mature store",
			in:    gcCounts{Matched: 130500, Orphans: 500, TooYoung: 12},
			trips: false,
			why:   "one interrupted multi-gigabyte file orphans ~10^3 blocks legitimately; 1 TB is ~131k objects",
		},
		{
			name:  "a young store after an interrupted first import",
			in:    gcCounts{Matched: 100, Orphans: 400},
			trips: true,
			why:   "the accepted false trip — legitimate, but it is a moment the operator should be looking at",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := checkGCGuard(tt.in)
			if (got != nil) != tt.trips {
				t.Fatalf("checkGCGuard(%+v) tripped = %v, want %v: %s", tt.in, got != nil, tt.trips, tt.why)
			}
			if got == nil {
				return
			}
			if got.Matched != tt.in.Matched || got.Orphans != tt.in.Orphans || got.TooYoung != tt.in.TooYoung {
				t.Errorf("guard error lost the counts: %+v, want %+v", got, tt.in)
			}
			if got.Classified() != tt.in.Matched+tt.in.Orphans {
				t.Errorf("Classified() = %d, want %d — a too-young object is evidence for neither side",
					got.Classified(), tt.in.Matched+tt.in.Orphans)
			}
			if got.Reason == "" {
				t.Error("guard error has no reason; the operator has to be told which clause fired")
			}
		})
	}
}

// TestGCOptionsDefaultToGuarded pins the polarity, which is the whole design:
// the guard is on in the zero value, so a caller that knows nothing about it is
// protected. Inverting this field — skipGuard, enableGuard — would silently
// unguard every call site that predates the change, which is precisely how the
// bug it defends against came to exist.
func TestGCOptionsDefaultToGuarded(t *testing.T) {
	if (gcOptions{}).allowMassDelete {
		t.Fatal("the zero value of gcOptions disables the guard; it must enable it")
	}
}
