package blob

import (
	"time"
)

type BlobSource interface {
	// Load loads blobs from the source.
	// It is a async operation which returns a Status object
	// immediately.
	Load() LoadStatus
}

type LoadStatus interface {
	ID() string
	StartTime() time.Time
	Duration() time.Duration
	Count() int
	Size() int64
	ErrorCount() int
	Blob() chan Blob
	Done() chan struct{}
}
