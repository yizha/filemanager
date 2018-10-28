package blob

import (
	"encoding/json"
	"sync/atomic"
	"time"

	"filemanager/logging"
)

// ProcessResult is a struct which implements the
// StoreResult/ScanResult interface along with other
// useful functions, it is meant to be used by the
// actual Storage implementation.
type ProcessStatus struct {
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
	doneChan   chan struct{}
	done       *int32
}

type processType string

const (
	processTypeLoad  processType = "load"
	processTypeStore             = "store"
)

func NewProcessStatus(id string, typ processType) *ProcessStatus {
	return &ProcessStatus{
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
		doneChan:   make(chan struct{}),
		done:       new(int32),
	}
}

func NewLoadStatus(id string) *ProcessStatus {
	return NewProcessStatus(id, processTypeLoad)
}

func NewStoreStatus(id string) *ProcessStatus {
	return NewProcessStatus(id, processTypeStore)
}

// ID returns the id of this process result
func (r *ProcessStatus) ID() string {
	return r.id
}

// Type returns type of this process result
func (r *ProcessStatus) Type() string {
	return string(r.typ)
}

// StartTime returns the process start time.
func (r *ProcessStatus) StartTime() time.Time {
	return r.startTime
}

// Duration returns the duration from its start time.
func (r *ProcessStatus) Duration() time.Duration {
	if atomic.LoadInt32(r.done) == int32(1) {
		if r.duration < 0 {
			r.duration = r.finishTime.Sub(r.startTime)
		}
		return r.duration
	}
	return time.Now().UTC().Sub(r.startTime)
}

// Count returns the processed blob number.
func (r *ProcessStatus) Count() int {
	return int(atomic.LoadInt64(r.count))
}

// Size returns the total size of processed blobs, in bytes.
func (r *ProcessStatus) Size() int64 {
	return atomic.LoadInt64(r.size)
}

// SkipCount returns the number of skipped blobs.
func (r *ProcessStatus) SkipCount() int {
	return int(atomic.LoadInt64(r.skipCount))
}

// SkipSize returns the total size of skipped blobs, in bytes.
func (r *ProcessStatus) SkipSize() int64 {
	return atomic.LoadInt64(r.skipSize)
}

// ErrorCount returns the number of process errors.
func (r *ProcessStatus) ErrorCount() int {
	return int(atomic.LoadInt64(r.errorCount))
}

// Blob returns a blob channel from which processed can be read.
func (r *ProcessStatus) Blob() chan Blob {
	return r.blobChan
}

// Done return a bool to indicate the process finishes or not.
func (r *ProcessStatus) Done() chan struct{} {
	return r.doneChan
}

// AddCount increases the blob count by the given number.
func (r *ProcessStatus) AddCount(n int) {
	atomic.AddInt64(r.count, int64(n))
}

// AddSize increases the blob size by the given number.
func (r *ProcessStatus) AddSize(n int64) {
	atomic.AddInt64(r.size, n)
}

// AddSkipCount increases the blob skip count by the given number.
func (r *ProcessStatus) AddSkipCount(n int) {
	atomic.AddInt64(r.skipCount, int64(n))
}

// AddSkipSize increases the blob skip size by the given number.
func (r *ProcessStatus) AddSkipSize(n int64) {
	atomic.AddInt64(r.skipSize, n)
}

// AddErrorCount increases the error count by the given number.
func (r *ProcessStatus) AddErrorCount(n int) {
	atomic.AddInt64(r.errorCount, int64(n))
}

// Finish sets the finish time, closes the blob channel, the
// error channel and the done channel.
func (r *ProcessStatus) Finish() {
	r.finishTime = time.Now().UTC()
	atomic.StoreInt32(r.done, int32(1))
	close(r.blobChan)
	close(r.doneChan)
}

// JSONStr returns json encoded string of the stats data
// this object carries.
func (r *ProcessStatus) JSONStr() string {
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
		logging.GetLogger().
			Error().
			Err(err).
			Msg("process result json encode failure")
		return "{}"
	}
	return string(data)
}
