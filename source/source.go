package source

import (
	"time"

	"filemanager/blob"
)

type Source interface {

	// Load loads blobs from the source.
	// It is a async operation which returns a Result object
	// immediately.
	Load() Status
}

type Status interface {
	ID() string
	StartTime() time.Time
	Duration() time.Duration
	Count() int
	Size() int64
	ErrorCount() int
	Blob() chan blob.Blob
	Done() chan struct{}
	JSONStr() string
}
