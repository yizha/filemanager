package fs

import (
	"bytes"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"filemanager/storage"
	"filemanager/util"

	"github.com/rs/zerolog"
	"github.com/satori/go.uuid"
)

type blobReadCloser struct {
	b *bytes.Buffer
}

func (b *blobReadCloser) Read(p []byte) (n int, err error) {
	return b.b.Read(p)
}
func (b *blobReadCloser) Close() error {
	b.b = nil
	return nil
}

func newBlobReadCloser(b []byte) *blobReadCloser {
	return &blobReadCloser{bytes.NewBuffer(b)}
}

// Blob represents a file on the file system.
type Blob struct {
	path string
	url  *url.URL
	name string
	blob []byte
	size int64
	hash *util.Hash
}

// Path returns the full path to the file
func (f *Blob) Path() string {
	return f.path
}

// Type returns storage.FileBlob
func (f *Blob) Type() storage.BlobType {
	return storage.FileBlob
}

// Location returns the full path to the file on file system
func (f *Blob) Url() *url.URL {
	return f.url
}

// Filename returns the file name of the blob.
func (f *Blob) Name() string {
	return f.name
}

// Size returns the file size, in bytes.
func (f *Blob) Size() (int64, error) {
	return f.size, nil
}

// Load loads the file content from underlying storage and stores
// it as []byte in memory, it also saves its size and calculate
// its hash
func (f *Blob) load() error {
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

// Bytes returns the file content as [] byte
func (f *Blob) Bytes() []byte {
	return f.blob
}

// Reader returns a bytes.Buffer wrapping its blob
func (f *Blob) ReadCloser() (io.ReadCloser, error) {
	if f.blob == nil {
		return nil, fmt.Errorf("underlying blob ([]byte) is nil")
	}
	return newBlobReadCloser(f.blob), nil
}

// Hash returns a *util.Hash object represents the hash
// of the file content.
func (f *Blob) Hash() (*util.Hash, error) {
	return f.hash, nil
}

type skipFunc func(string) bool

// Storage represents a directory in the file system.
type Storage struct {

	// path to the root directory
	root string

	// max number of go routines to load file
	maxLoader int

	// max number of go routines to save file
	maxSaver int

	// filter func to skip certain path
	skip skipFunc

	// logger
	lg *zerolog.Logger
}

// New creates a file system storage
func New(
	root string,
	maxSaver int,
	maxLoader int,
	lg *zerolog.Logger) (storage.Storage, error) {

	root, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if maxSaver < 1 || maxSaver > 20 {
		return nil, fmt.Errorf("maxSaver %d is out of allowed range [1, 20]", maxSaver)
	}
	if maxLoader < 1 || maxLoader > 20 {
		return nil, fmt.Errorf("maxLoader %d is out of allowed range [1, 20]", maxLoader)
	}
	l := lg.With().Str("root", root).Logger()
	return &Storage{
		root:      root,
		maxLoader: maxLoader,
		maxSaver:  maxSaver,
		skip:      func(s string) bool { return false },
		lg:        &l,
	}, nil
}

func loadFile(
	id int,
	fileCh chan string,
	wg *sync.WaitGroup,
	pr *storage.ProcessResult,
	lg *zerolog.Logger) {

	l := lg.With().Int("loader", id).Logger()

	l.Debug().Msg("started")

	blobCh := pr.Blob()
	for fpath := range fileCh {
		url := util.PathToUrl(fpath)
		bl := l.With().Str("url", url.String()).Logger()
		blob := &Blob{
			path: fpath,
			name: filepath.Base(fpath),
			url:  url,
		}

		bl.Debug().Msg("start loading file")
		t := time.Now()
		err := blob.load()
		if err != nil {
			pr.AddErrorCount(1)
			bl.Error().Err(err).Msg("load")
			continue
		}
		h, _ := blob.Hash()
		size, _ := blob.Size()
		pr.AddCount(1)
		pr.AddSize(size)
		bl.Info().
			Str("content-hash", h.String()).
			Int64("size", size).
			Int64("duration", time.Now().Sub(t).Nanoseconds()).
			Msg("loaded")
		blobCh <- blob
	}
	wg.Done()

	l.Debug().Msg("finished")
}

func _scan(
	dirPath string,
	skip skipFunc,
	fileCh chan string,
	pr *storage.ProcessResult,
	lg *zerolog.Logger) {

	lg.Debug().
		Str("path", dirPath).
		Msg("start scanning dir")

	dir, err := os.Open(dirPath)
	if err != nil {
		pr.AddErrorCount(1)
		lg.Error().
			Err(err).
			Str("path", dirPath).
			Msg("open dir error")
		return
	}
	fileInfoArray, err := dir.Readdir(-1)
	dir.Close()
	if err != nil {
		pr.AddErrorCount(1)
		lg.Error().
			Err(err).
			Str("path", dirPath).
			Msg("read dir error")
	}
	if fileInfoArray != nil {
		for _, fi := range fileInfoArray {
			fpath := filepath.Join(dirPath, fi.Name())
			if skip(fpath) {
				pr.AddSkipCount(1)
				if fi.IsDir() {
					lg.Info().
						Str("path", fpath).
						Msg("skip dir")
				} else {
					size := fi.Size()
					pr.AddSkipSize(size)
					lg.Info().
						Str("path", fpath).
						Int64("size", size).
						Msg("skip file")
				}
			} else {
				if fi.IsDir() {
					_scan(fpath, skip, fileCh, pr, lg)
				} else {
					fileCh <- fpath
				}
			}
		}
	}

	lg.Debug().Str("path", dirPath).Msg("done scanning dir")
}

func scan(
	dirPath string,
	loaderCnt int,
	skip skipFunc,
	pr *storage.ProcessResult,
	lg *zerolog.Logger) {

	lg.Debug().Msg("start scanning")

	fileCh := make(chan string)
	wg := &sync.WaitGroup{}
	wg.Add(loaderCnt)
	for i := 0; i < loaderCnt; i++ {
		go loadFile(i, fileCh, wg, pr, lg)
	}
	_scan(dirPath, skip, fileCh, pr, lg)
	close(fileCh)
	wg.Wait()
	pr.Finish()

	lg.Debug().Msg("done scanning")
}

func blobExists(path string, size int64) (bool, error) {
	fi, err := os.Stat(path)
	if err == nil {
		return fi.Size() == size, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

type blobMeta struct {
	ContentHash string `json:"content-hash"`
	Size        int64  `json:"size"`
	Filename    string `json:"filename,omitempty"`
}

func newBlobMeta(h string, size int64, name string) ([]byte, error) {
	return json.Marshal(blobMeta{
		ContentHash: h,
		Size:        size,
		Filename:    name,
	})
}

func saveFile(path string, src io.ReadCloser) (int64, error) {
	dst, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return 0, err
	}
	defer dst.Close()
	defer src.Close()
	return io.Copy(dst, src)
}

func save(
	id int,
	root string,
	inCh chan storage.Blob,
	wg *sync.WaitGroup,
	pr *storage.ProcessResult,
	lg *zerolog.Logger) {

	l := lg.With().Int("saver", id).Logger()

	l.Debug().Msg("started")

	outCh := pr.Blob()
	for blob := range inCh {
		url := blob.Url()
		bl := l.With().Str("source", url.String()).Logger()

		// blob hash
		blobHash, err := blob.Hash()
		if err != nil {
			pr.AddErrorCount(1)
			bl.Error().Err(err).Msg("get source hash error")
			continue
		}
		alg := blobHash.Algorithm()
		hex := blobHash.Hex()
		dir := filepath.Join(alg, hex[0:2], hex[2:4], hex[4:6], hex[6:8], hex)

		blobDirPath := filepath.Join(root, dir)
		metaPath := filepath.Join(blobDirPath, "meta")
		blobPath := filepath.Join(blobDirPath, "blob")

		blobHashStr := blobHash.String()
		bl = bl.With().
			Str("content-hash", blobHashStr).
			Str("target-dir", blobDirPath).
			Logger()

		blobSize, err := blob.Size()
		if err != nil {
			pr.AddErrorCount(1)
			bl.Error().Err(err).Msg("get blob size error")
			continue
		}

		// skip if target already exists
		exists, err := blobExists(blobPath, blobSize)
		if err != nil {
			pr.AddErrorCount(1)
			bl.Error().Err(err).Msg("check target blob error")
			continue
		}
		if exists {
			pr.AddSkipCount(1)
			pr.AddSkipSize(blobSize)
			bl.Info().Msg("skip existing")
			continue
		}

		bl.Debug().Msg("start saving")

		t := time.Now()

		// create dir
		err = os.MkdirAll(blobDirPath, 0755)
		if err != nil {
			pr.AddErrorCount(1)
			bl.Error().Err(err).Msg("create target dir error")
			continue
		}

		// save meta file
		metaData, err := newBlobMeta(blobHashStr, blobSize, blob.Name())
		if err != nil {
			pr.AddErrorCount(1)
			bl.Error().Err(err).Msg("meta generate error")
			continue
		}
		bytesWritten, err := saveFile(metaPath, newBlobReadCloser(metaData))
		if err != nil {
			pr.AddErrorCount(1)
			bl.Error().Err(err).Msg("meta file save error")
			continue
		}
		bl.Info().
			Str("meta-path", metaPath).
			Int64("meta-size", bytesWritten).
			Msg("blob meta written")
		totalWritten := bytesWritten

		// save blob file
		blobReadCloser, err := blob.ReadCloser()
		if err != nil {
			pr.AddErrorCount(1)
			bl.Error().Err(err).Msg("blob reader error")
			continue
		}
		bytesWritten, err = saveFile(blobPath, blobReadCloser)
		if err != nil {
			pr.AddErrorCount(1)
			bl.Error().Err(err).Msg("blob file save error")
			continue
		}
		bl.Info().
			Str("blob-path", blobPath).
			Int64("blob-size", bytesWritten).
			Msg("blob file written")
		totalWritten = totalWritten + bytesWritten

		// log result
		pr.AddCount(1)
		pr.AddSize(blobSize)
		bl.Info().
			Int64("size", totalWritten).
			Int64("duration", time.Now().Sub(t).Nanoseconds()).
			Msg("done saving")

		outCh <- &Blob{
			path: blobPath,
			url:  util.PathToUrl(blobPath),
			name: blob.Name(),
			blob: nil,
			size: blobSize,
			hash: blobHash,
		}
	}
	wg.Done()

	l.Debug().Msg("finished")
}

// Scan scans the storage and returns a storage.ScanResult
// which can be quiried for status data and read file blobs.
func (s *Storage) Scan() storage.ScanResult {
	id := uuid.Must(uuid.NewV4()).String()
	result := storage.NewScanResult(id)
	l := s.lg.With().
		Str("process-id", id).
		Str("process-name", "scan").
		Logger()
	go scan(s.root, s.maxLoader, s.skip, result, &l)
	return result
}

func store(
	dirPath string,
	saverCnt int,
	ch chan storage.Blob,
	pr *storage.ProcessResult,
	lg *zerolog.Logger) {

	lg.Debug().Msg("start saving")

	wg := &sync.WaitGroup{}
	wg.Add(saverCnt)
	for i := 0; i < saverCnt; i++ {
		go save(i, dirPath, ch, wg, pr, lg)
	}
	wg.Wait()
	pr.Finish()

	lg.Debug().Msg("done saving")
}

// Store stores blobs from the given channel to the file system.
func (s *Storage) Store(blobCh chan storage.Blob) storage.StoreResult {
	id := uuid.Must(uuid.NewV4()).String()
	result := storage.NewStoreResult(id)
	l := s.lg.With().
		Str("process-id", id).
		Str("process-name", "store").
		Logger()
	go store(s.root, s.maxSaver, blobCh, result, &l)
	return result
}
