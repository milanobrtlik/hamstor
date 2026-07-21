package s3store

import "github.com/google/uuid"

// NewKey generates a prefixed S3 key: "{first-2-hex-chars}/{uuid}".
func NewKey() string {
	id := uuid.New().String()
	return id[:2] + "/" + id
}
