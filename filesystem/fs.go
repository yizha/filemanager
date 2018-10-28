package filesystem

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"filemanager/blob"
	"filemanager/util"

	"github.com/rs/zerolog"
	"github.com/satori/go.uuid"
)

type skipFunc func(string) bool

var skipDotFile = func(path string) bool {
	name := filepath.Base(path)
	if len(name) <= 0 {
		return true
	}
	if name[0:1] == "." {
		return true
	}
	return false
}

func composeSkipFunc(funcs ...skipFunc) skipFunc {
	return func(path string) bool {
		for _, f := range funcs {
			if f(path) {
				return true
			}
		}
		return false
	}
}

type FileSystem struct {
	root      string
	maxLoader int
	maxSaver  int
	skip      skipFunc
	lg        *zerolog.Logger
}

// New creates a file system storage
func New(
	root string,
	maxSaver int,
	maxLoader int,
	lg *zerolog.Logger) (*FileSystem, error) {

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
	return &FileSystem{
		root:      root,
		skip:      skipDotFile,
		maxLoader: maxLoader,
		maxSaver:  maxSaver,
		lg:        &l,
	}, nil
}

func loadFile(
	id int,
	fileCh chan string,
	wg *sync.WaitGroup,
	pr *blob.ProcessStatus,
	lg *zerolog.Logger) {

	defer wg.Done()

	l := lg.With().Int("loader", id).Logger()

	l.Debug().Msg("started")

	blobCh := pr.Blob()
	for fpath := range fileCh {
		blob := NewFileBlob(fpath)
		url := blob.Url()
		bl := l.With().Str("url", url.String()).Logger()

		bl.Debug().Msg("start loading file")
		t := time.Now()
		err := blob.Load()
		if err != nil {
			pr.AddErrorCount(1)
			bl.Error().Err(err).Msg("load")
			continue
		}
		h := blob.Hash()
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

	l.Debug().Msg("finished")
}

func walkDir(
	dirPath string,
	skip skipFunc,
	fileCh chan string,
	sts *blob.ProcessStatus,
	lg *zerolog.Logger) {

	lg.Debug().
		Str("path", dirPath).
		Msg("start scanning dir")

	dir, err := os.Open(dirPath)
	if err != nil {
		sts.AddErrorCount(1)
		lg.Error().
			Err(err).
			Str("path", dirPath).
			Msg("open dir error")
		return
	}
	fileInfoArray, err := dir.Readdir(-1)
	dir.Close()
	if err != nil {
		sts.AddErrorCount(1)
		lg.Error().
			Err(err).
			Str("path", dirPath).
			Msg("read dir error")
	}
	if fileInfoArray != nil {
		for _, fi := range fileInfoArray {
			fpath := filepath.Join(dirPath, fi.Name())
			if skip(fpath) {
				sts.AddSkipCount(1)
				if fi.IsDir() {
					lg.Info().
						Str("path", fpath).
						Msg("skip dir")
				} else {
					size := fi.Size()
					sts.AddSkipSize(size)
					lg.Info().
						Str("path", fpath).
						Int64("size", size).
						Msg("skip file")
				}
			} else {
				if fi.IsDir() {
					walkDir(fpath, skip, fileCh, sts, lg)
				} else {
					fileCh <- fpath
				}
			}
		}
	}

	lg.Debug().Str("path", dirPath).Msg("done scanning dir")
}

func load(
	dirPath string,
	skip skipFunc,
	loaderCnt int,
	sts *blob.ProcessStatus,
	lg *zerolog.Logger) {

	lg.Debug().Msg("start scanning")

	fileCh := make(chan string)
	wg := &sync.WaitGroup{}
	wg.Add(loaderCnt)
	for i := 0; i < loaderCnt; i++ {
		go loadFile(i, fileCh, wg, sts, lg)
	}
	walkDir(dirPath, skip, fileCh, sts, lg)
	close(fileCh)
	wg.Wait()
	sts.Finish()

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
	inCh chan blob.Blob,
	wg *sync.WaitGroup,
	sts *blob.ProcessStatus,
	lg *zerolog.Logger) {

	defer wg.Done()

	l := lg.With().Int("saver", id).Logger()

	l.Debug().Msg("started")

	outCh := sts.Blob()
	for blob := range inCh {
		url := blob.Url()
		bl := l.With().Str("source", url.String()).Logger()

		// blob size
		blobSize, err := blob.Size()
		if err != nil {
			sts.AddErrorCount(1)
			bl.Error().Err(err).Msg("get blob size error")
			continue
		}

		// blob hash & path
		blobHash := blob.Hash()
		alg := blobHash.Algorithm()
		hex := blobHash.Hex()
		blobPath := filepath.Join(root, alg, hex[0:2], hex[2:4], hex[4:6], hex[6:8], hex)
		blobHashStr := blobHash.String()
		bl = bl.With().
			Str("content-hash", blobHashStr).
			Str("blob-path", blobPath).
			Logger()

		// skip if target already exists
		exists, err := blobExists(blobPath, blobSize)
		if err != nil {
			sts.AddErrorCount(1)
			bl.Error().Err(err).Msg("check target blob error")
			continue
		}
		if exists {
			sts.AddSkipCount(1)
			sts.AddSkipSize(blobSize)
			bl.Info().Msg("skip existing")
			continue
		}

		bl.Debug().Msg("start saving")

		t := time.Now()

		// create dir
		err = os.MkdirAll(filepath.Dir(blobPath), 0755)
		if err != nil {
			sts.AddErrorCount(1)
			bl.Error().Err(err).Msg("mkdir error")
			continue
		}

		// save blob file
		blobReadCloser, err := blob.ReadCloser()
		if err != nil {
			sts.AddErrorCount(1)
			bl.Error().Err(err).Msg("blob reader error")
			continue
		}
		_, err = saveFile(blobPath, blobReadCloser)
		if err != nil {
			sts.AddErrorCount(1)
			bl.Error().Err(err).Msg("save blob error")
			continue
		}
		bl.Info().
			Int64("blob-size", blobSize).
			Msg("blob written")

		// log result
		sts.AddCount(1)
		sts.AddSize(blobSize)
		bl.Info().
			Int64("duration", time.Now().Sub(t).Nanoseconds()).
			Msg("done saving")

		outCh <- &FileBlob{
			path: blobPath,
			url:  util.PathToUrl(blobPath),
			name: blob.Name(),
			blob: nil,
			size: blobSize,
			hash: blobHash,
		}
	}

	l.Debug().Msg("finished")
}

func store(
	dirPath string,
	saverCnt int,
	ch chan blob.Blob,
	sts *blob.ProcessStatus,
	lg *zerolog.Logger) {

	lg.Debug().Msg("start storing")

	wg := &sync.WaitGroup{}
	wg.Add(saverCnt)
	for i := 0; i < saverCnt; i++ {
		go save(i, dirPath, ch, wg, sts, lg)
	}
	wg.Wait()
	sts.Finish()

	lg.Debug().Msg("done storing")
}

func (fs *FileSystem) Load() blob.LoadStatus {
	id := uuid.Must(uuid.NewV4()).String()
	sts := blob.NewLoadStatus(id)
	l := fs.lg.With().Str("load-id", id).Logger()
	go load(fs.root, fs.skip, fs.maxLoader, sts, &l)
	return sts
}

func (fs *FileSystem) Store(blobCh chan blob.Blob) blob.StoreStatus {
	id := uuid.Must(uuid.NewV4()).String()
	sts := blob.NewStoreStatus(id)
	l := fs.lg.With().Str("process-id", id).Logger()
	go store(fs.root, fs.maxSaver, blobCh, sts, &l)
	return sts
}
