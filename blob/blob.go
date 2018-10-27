package blob

import (
	"bytes"
	"io"
	"net/url"

	"filemanager/util"
)

// BlobType represents the type of the blob
type Type string

// supported blob types
const (
	TypeFile Type = "file"
)

// Blob represents a []byte object with
type Blob interface {

	// return the hash of the blob
	Hash() *util.Hash

	// return the type
	Type() Type

	// return the url to the blob
	Url() *url.URL

	// return the name of the blob
	Name() string

	// return the blob size, in bytes
	// or error if something goes wrong
	// while trying to get the blob size
	Size() (int64, error)

	// return the actual data as an io.Reader
	// or error if something goes wrong
	// while initializing the reader
	ReadCloser() (io.ReadCloser, error)
}

type BufferedReadCloser struct {
	b *bytes.Buffer
}

func (b *BufferedReadCloser) Read(p []byte) (n int, err error) {
	return b.b.Read(p)
}

// Close clears the cached []byte. A BufferedReadCloser
// MUST NOT be used after Close() is called.
func (b *BufferedReadCloser) Close() error {
	b.b = nil
	return nil
}

func NewBufferedReadCloser(b []byte) *BufferedReadCloser {
	return &BufferedReadCloser{bytes.NewBuffer(b)}
}
