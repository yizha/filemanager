package filesystem

import (
	"crypto/sha1"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"

	"filemanager/blob"
	"filemanager/util"
)

// Blob represents a file on the file system.
type FileBlob struct {
	path string
	url  *url.URL
	name string
	blob []byte
	size int64
	hash *util.Hash
}

// Path returns the full path to the file
func (f *FileBlob) Path() string {
	return f.path
}

// Type returns storage.FileBlob
func (f *FileBlob) Type() blob.Type {
	return blob.TypeFile
}

// Location returns the full path to the file on file system
func (f *FileBlob) Url() *url.URL {
	return f.url
}

// Filename returns the file name of the blob.
func (f *FileBlob) Name() string {
	return f.name
}

// Size returns the file size, in bytes.
func (f *FileBlob) Size() (int64, error) {
	return f.size, nil
}

// Load loads the file content from underlying storage and stores
// it ([]byte) in memory, it also saves its size and calculates
// its hash.
func (f *FileBlob) Load() error {
	if f.blob != nil {
		f.size = int64(len(f.blob))
		h := sha1.Sum(f.blob)
		f.hash = util.NewSha1Hash(h[:])
		return nil
	}
	ff, err := os.Open(f.path)
	if err != nil {
		return err
	}
	defer ff.Close()
	blob, err := ioutil.ReadAll(ff)
	if err != nil {
		return err
	}
	f.blob = blob
	f.size = int64(len(blob))
	h := sha1.Sum(blob)
	f.hash = util.NewSha1Hash(h[:])
	return nil
}

// Free clear the cached []byte
func (f *FileBlob) Free() {
	f.blob = nil
}

// Bytes returns the file content as [] byte
func (f *FileBlob) Bytes() []byte {
	return f.blob
}

// Reader returns a bytes.Buffer wrapping its blob
func (f *FileBlob) ReadCloser() (io.ReadCloser, error) {
	if f.blob == nil {
		return nil, fmt.Errorf("underlying blob ([]byte) is nil")
	}
	return blob.NewBufferedReadCloser(f.blob), nil
}

// Hash returns a *util.Hash object represents the hash
// of the file content.
func (f *FileBlob) Hash() *util.Hash {
	return f.hash
}

func NewFileBlob(path string) *FileBlob {
	url := util.PathToUrl(path)
	return &FileBlob{
		path: path,
		url:  url,
		name: filepath.Base(path),
	}
}
