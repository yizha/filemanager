package fs

import (
	"bytes"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"

	"filemanager/storage"
	"filemanager/util"

	"github.com/rs/zerolog"
	"github.com/satori/go.uuid"
)

// Blob represents a file on the file system.
type Blob struct {
	filename string
	blob     []byte
	size     int64
	hash     *util.Hash
}

// Type returns storage.FileBlob
func (f *Blob) Type() storage.BlobType {
	return storage.FileBlob
}

// Filename returns the file name of the blob.
func (f *Blob) Filename() string {
	return f.filename
}

// Size returns the file size, in bytes.
func (f *Blob) Size() int64 {
	if f.size == 0 {
		f.size = int64(len(f.Bytes()))
	}
	return f.size
}

// Bytes return the file content as []byte.
func (f *Blob) Bytes() []byte {
	if f.blob == nil {
		f.blob = make([]byte, 0, 0)
	}
	return f.blob
}

// Save saves the file content to the given Writer,
// it returns the number of bytes written and error
// if there is.
func (f *Blob) Save(w io.Writer) (int64, error) {
	return io.Copy(w, bytes.NewBuffer(f.Bytes()))
}

// Hash returns a *util.Hash object represents the hash
// of the file content.
func (f *Blob) Hash() *util.Hash {
	if f.hash == nil {
		h := sha1.Sum(f.Bytes())
		f.hash = util.NewSha1Hash(h[:])
	}
	return f.hash
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
func New(root string, maxSaver, maxLoader int, lg *zerolog.Logger) (storage.Storage, error) {
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

func load(
	id int,
	fileCh chan string,
	wg *sync.WaitGroup,
	pr *storage.ProcessResult,
	lg *zerolog.Logger) {

	l := lg.With().Int("loader", id).Logger()

	l.Debug().Msg("started")

	blobCh := pr.Blob()
	for fpath := range fileCh {
		l.Debug().Str("path", fpath).Msg("start loading file")
		t := time.Now()
		f, err := os.Open(fpath)
		if err != nil {
			pr.AddErrorCount(1)
			l.Error().
				Err(err).
				Str("path", fpath).
				Msg("open file error")
			continue
		}
		data, err := ioutil.ReadAll(f)
		f.Close()
		if err != nil {
			pr.AddErrorCount(1)
			l.Error().
				Err(err).
				Str("path", fpath).
				Int64("duration", time.Now().Sub(t).Nanoseconds()).
				Msg("read file error")
		} else {
			size := int64(len(data))
			pr.AddCount(1)
			pr.AddSize(size)
			blob := &Blob{
				filename: filepath.Base(fpath),
				blob:     data,
				size:     size,
				hash:     nil,
			}
			l.Info().
				Str("path", fpath).
				Str("content-hash", blob.Hash().String()).
				Int64("size", size).
				Int64("duration", time.Now().Sub(t).Nanoseconds()).
				Msg("done loading file")
			blobCh <- blob

		}
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
		go load(i, fileCh, wg, pr, lg)
	}
	_scan(dirPath, skip, fileCh, pr, lg)
	close(fileCh)
	wg.Wait()
	pr.Finish()

	lg.Debug().Msg("done scanning")
}

func blobDirPath(f storage.Blob) string {
	h := f.Hash()
	hex := h.Hex()
	return filepath.Join(h.Algorithm(), hex[0:2], hex[2:4], hex[4:6], hex[6:8], hex)
}

func blobExists(dir string, size int64) (bool, error) {
	fi, err := os.Stat(dir)
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

func meta(b storage.Blob) ([]byte, error) {
	return json.Marshal(blobMeta{
		ContentHash: b.Hash().String(),
		Size:        int64(len(b.Bytes())),
		Filename:    b.Filename(),
	})
}

func save(
	id int,
	root string,
	blobCh chan storage.Blob,
	wg *sync.WaitGroup,
	pr *storage.ProcessResult,
	lg *zerolog.Logger) {

	l := lg.With().Int("saver", id).Logger()

	l.Debug().Msg("started")

	for blob := range blobCh {
		blobDir := filepath.Join(root, blobDirPath(blob))
		metaPath := filepath.Join(blobDir, "meta")
		blobPath := filepath.Join(blobDir, "blob")
		bl := l.With().
			Str("content-hash", blob.Hash().String()).
			Str("blob-dir", blobDir).
			Logger()
		data := blob.Bytes()
		blobSize := int64(len(data))

		// skip if target already exists
		exists, err := blobExists(blobPath, blobSize)
		if err != nil {
			pr.AddErrorCount(1)
			bl.Error().Err(err).Msg("check duplication error")
			continue
		}
		if exists {
			pr.AddSkipCount(1)
			pr.AddSkipSize(blobSize)
			bl.Info().Msg("skip duplicate")
			continue
		}

		bl.Debug().Msg("start saving")

		t := time.Now()

		// create dir
		err = os.MkdirAll(blobDir, 0755)
		if err != nil {
			pr.AddErrorCount(1)
			bl.Error().Err(err).Msg("create dir error")
			continue
		}

		// save meta file
		metaData, err := meta(blob)
		if err != nil {
			pr.AddErrorCount(1)
			bl.Error().Err(err).Msg("generate metadata error")
			continue
		}
		err = ioutil.WriteFile(metaPath, metaData, 0644)
		if err != nil {
			pr.AddErrorCount(1)
			bl.Error().Err(err).Msg("save meta file error")
			continue
		}

		// save blob file
		err = ioutil.WriteFile(blobPath, data, 0644)
		if err != nil {
			pr.AddErrorCount(1)
			bl.Error().Err(err).Msg("save blob file error")
			continue
		}

		// log result
		pr.AddCount(1)
		pr.AddSize(blobSize)
		bl.Info().
			Int64("size", blobSize).
			Int64("duration", time.Now().Sub(t).Nanoseconds()).
			Msg("done saving")

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
func (s *Storage) Store(ch chan storage.Blob) storage.StoreResult {
	id := uuid.Must(uuid.NewV4()).String()
	result := storage.NewStoreResult(id)
	l := s.lg.With().
		Str("process-id", id).
		Str("process-name", "store").
		Logger()
	go store(s.root, s.maxSaver, ch, result, &l)
	return result
}
