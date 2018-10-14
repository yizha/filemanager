package storage

import (
	"encoding/json"
	"io"
	"sync/atomic"
	"time"

	"filemanager/util"

	"github.com/yizha/go/logging"
)

// BlobType represents the type of the blob
type BlobType int

// supported blob types
const (
	FileBlob BlobType = iota
)

// Blob represents a []byte object with
type Blob interface {
	// return the type
	Type() BlobType

	// return the blob filename
	Filename() string

	// return the blob size, in bytes
	Size() int64

	// return the actual data
	Bytes() []byte

	// Save saves the blob to given Writer
	// returns bytes written and error (or nil)
	Save(io.Writer) (int64, error)

	// return the hash of the blob
	Hash() *util.Hash
}

// StoreResult provides functions to get various data
// of store operation.
type StoreResult interface {
	// ID() returns the id of the operation
	ID() string

	// start timestamp of the process
	StartTime() time.Time

	// duration of the process
	Duration() time.Duration

	// count of processed blobs
	Count() int

	// size of processed blobs, in bytes
	Size() int64

	// count of skipped blobs
	SkipCount() int

	// size of all skipped blobs, in bytes
	SkipSize() int64

	// count of process errors
	ErrorCount() int

	// the blob channel, from which reads the blob
	Blob() chan Blob

	// the returned is closed when the operation completes
	Done() chan int

	// returns a string representation of above data
	JSONStr() string
}

// ScanResult provides functions to get various data
// of the scan operation.
type ScanResult interface {
	StoreResult
}

// ProcessResult is a struct which implements the
// StoreResult/ScanResult interface along with other
// useful functions, it is meant to be used by the
// actual Storage implementation.
type ProcessResult struct {
	id         string
	typ        processType
	startTime  time.Time
	finishTime time.Time
	duration   time.Duration
	count      *int64
	size       *int64
	skipCount  *int64
	skipSize   *int64
	errorCount *int64
	blobChan   chan Blob
	doneChan   chan int
	done       *int32
}

type processType string

const (
	processTypeScan  processType = "scan"
	processTypeStore             = "store"
)

func newProcessResult(id string, typ processType) *ProcessResult {
	return &ProcessResult{
		id:         id,
		typ:        typ,
		startTime:  time.Now().UTC(),
		finishTime: time.Now().UTC().Add(-time.Second * 60),
		duration:   -1,
		count:      new(int64),
		size:       new(int64),
		skipCount:  new(int64),
		skipSize:   new(int64),
		errorCount: new(int64),
		blobChan:   make(chan Blob),
		doneChan:   make(chan int),
		done:       new(int32),
	}
}

// NewScanResult creates a ProcessResult instance with given id
func NewScanResult(id string) *ProcessResult {
	return newProcessResult(id, processTypeScan)
}

// NewStoreResult creates a ProcessResult instance with given id
func NewStoreResult(id string) *ProcessResult {
	return newProcessResult(id, processTypeStore)
}

// ID returns the id of this process result
func (r *ProcessResult) ID() string {
	return r.id
}

// Type returns type of this process result
func (r *ProcessResult) Type() string {
	return string(r.typ)
}

// StartTime returns the process start time.
func (r *ProcessResult) StartTime() time.Time {
	return r.startTime
}

// Duration returns the duration from its start time.
func (r *ProcessResult) Duration() time.Duration {
	if atomic.LoadInt32(r.done) == int32(1) {
		if r.duration < 0 {
			r.duration = r.finishTime.Sub(r.startTime)
		}
		return r.duration
	}
	return time.Now().UTC().Sub(r.startTime)
}

// Count returns the processed blob number.
func (r *ProcessResult) Count() int {
	return int(atomic.LoadInt64(r.count))
}

// Size returns the total size of processed blobs, in bytes.
func (r *ProcessResult) Size() int64 {
	return atomic.LoadInt64(r.size)
}

// SkipCount returns the number of skipped blobs.
func (r *ProcessResult) SkipCount() int {
	return int(atomic.LoadInt64(r.skipCount))
}

// SkipSize returns the total size of skipped blobs, in bytes.
func (r *ProcessResult) SkipSize() int64 {
	return atomic.LoadInt64(r.skipSize)
}

// ErrorCount returns the number of process errors.
func (r *ProcessResult) ErrorCount() int {
	return int(atomic.LoadInt64(r.errorCount))
}

// Blob returns a blob channel from which processed can be read.
func (r *ProcessResult) Blob() chan Blob {
	return r.blobChan
}

// Done return a bool to indicate the process finishes or not.
func (r *ProcessResult) Done() chan int {
	return r.doneChan
}

// AddCount increases the blob count by the given number.
func (r *ProcessResult) AddCount(n int) {
	atomic.AddInt64(r.count, int64(n))
}

// AddSize increases the blob size by the given number.
func (r *ProcessResult) AddSize(n int64) {
	atomic.AddInt64(r.size, n)
}

// AddSkipCount increases the blob skip count by the given number.
func (r *ProcessResult) AddSkipCount(n int) {
	atomic.AddInt64(r.skipCount, int64(n))
}

// AddSkipSize increases the blob skip size by the given number.
func (r *ProcessResult) AddSkipSize(n int64) {
	atomic.AddInt64(r.skipSize, n)
}

// AddErrorCount increases the error count by the given number.
func (r *ProcessResult) AddErrorCount(n int) {
	atomic.AddInt64(r.errorCount, int64(n))
}

// Finish sets the finish time, closes the blob channel, the
// error channel and the done channel.
func (r *ProcessResult) Finish() {
	r.finishTime = time.Now().UTC()
	atomic.StoreInt32(r.done, int32(1))
	close(r.blobChan)
	close(r.doneChan)
}

// JSONStr returns json encoded string of the stats data
// this object carries.
func (r *ProcessResult) JSONStr() string {
	/*			id:         id,
	startTime:  time.Now().UTC(),
	finishTime: time.Now().UTC().Add(-time.Second * 60),
	duration:   -1,
	count:      new(int64),
	size:       new(int64),
	skipCount:  new(int64),
	skipSize:   new(int64),
	errorCount: new(int64),
	blobChan:   make(chan Blob),
	doneChan:   make(chan int),
	done:       new(int32),*/
	tsFmt := "2006-01-02T15:04:05.999999"
	done := atomic.LoadInt32(r.done) == int32(1)
	finishTs := ""
	if done {
		finishTs = r.finishTime.Format(tsFmt)
	}
	stats := struct {
		ID         string        `json:"id"`
		Type       string        `json:"type"`
		StartTime  string        `json:"start"`
		FinishTime string        `json:"finish,omitempty"`
		Duration   time.Duration `json:"duration"`
		Count      int64         `json:"count"`
		Size       int64         `json:"size"`
		SkipCount  int64         `json:"skip-count"`
		SkipSize   int64         `json:"skip-size"`
		ErrorCount int64         `json:"error-count"`
		Done       bool          `json:"done"`
	}{
		ID:         r.id,
		Type:       r.Type(),
		StartTime:  r.startTime.Format(tsFmt),
		FinishTime: finishTs,
		Duration:   r.Duration(),
		Count:      atomic.LoadInt64(r.count),
		Size:       atomic.LoadInt64(r.size),
		SkipCount:  atomic.LoadInt64(r.skipCount),
		SkipSize:   atomic.LoadInt64(r.skipSize),
		ErrorCount: atomic.LoadInt64(r.errorCount),
		Done:       done,
	}
	data, err := json.Marshal(stats)
	if err != nil {
		logging.GetLogger("main").
			Error().
			Err(err).
			Msg("process result json encode failure")
		return "{}"
	}
	return string(data)
}

// Storage is the interface for blob storage
type Storage interface {

	// Store stores all blobs from the given channel,
	// the function is async and returns immediately
	// the store progress and result can be quired by
	// invoking corresponding functions on the returned
	// StoreResult object.
	Store(chan Blob) StoreResult

	// Scan scans blobs from this storage. It is async and
	// returns immediately, the blobs can be read from the
	// blob channel in the returned ScanResult as well as
	// other useful counts/sizes.
	Scan() ScanResult
}
