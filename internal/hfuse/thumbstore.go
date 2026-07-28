package hfuse

import (
	"context"
	"log"

	"github.com/milan/hamstor/internal/db"
	"github.com/milan/hamstor/internal/s3store"
	"github.com/milan/hamstor/internal/thumb"
)

// storeThumb puts a rendered thumbnail in S3 so it outlives this machine.
//
// Without this, thumbnails are derived data that only ever existed in one
// user's local cache: a new machine regenerates every one of them by
// downloading every original, which for a block-stored photo is 8 MiB to
// produce 25 KB. The stored object is the two PNGs back to back, sealed as one
// message when encryption is on.
//
// Failure is never fatal and never reported upwards. A missing thumbnail is
// repaired by the desktop on demand; the caller has already written the local
// copy, which is what the user sees today.
func (hfs *HamstorFS) storeThumb(ctx context.Context, inodeID, mtimeNs int64, r thumb.Rendered) {
	if hfs.Store == nil || len(r.Normal) == 0 || len(r.Large) == 0 {
		return
	}

	body := make([]byte, 0, len(r.Normal)+len(r.Large))
	body = append(body, r.Normal...)
	body = append(body, r.Large...)
	normalLen := int64(len(r.Normal))

	if hfs.Encryptor != nil {
		sealed, err := hfs.Encryptor.Encrypt(body)
		if err != nil {
			log.Printf("hamstor: thumbnail encrypt for inode %d: %v", inodeID, err)
			return
		}
		body = sealed
	}

	key := s3store.NewKey()
	if err := hfs.Store.Upload(ctx, key, body); err != nil {
		log.Printf("hamstor: thumbnail upload for inode %d: %v", inodeID, err)
		return
	}

	ok, oldKey, err := hfs.DB.PutThumbnail(inodeID, db.ThumbRecord{
		S3Key:      key,
		Offset:     0,
		Length:     int64(len(body)),
		NormalLen:  normalLen,
		SrcMtimeNs: mtimeNs,
	})
	if err != nil {
		log.Printf("hamstor: thumbnail commit for inode %d: %v", inodeID, err)
	}
	if err != nil || !ok {
		// Nothing references the object: the file was overwritten or deleted
		// while this thumbnail was being rendered. Drop it rather than leave it
		// for GC — GC would get it eventually, but only after the grace period
		// and only on a run nobody may make.
		hfs.dropObjects(ctx, []string{key})
		return
	}
	if oldKey != "" {
		hfs.dropObjects(ctx, []string{oldKey})
	}
}
