package main

import (
	"fmt"
	"os"
	"path/filepath"

	fs "filemanager/filesystem"
	"filemanager/logging"
)

func main() {
	lg := logging.GetLogger()
	lg.Info().Msg("start testing ...")

	inCh := make(chan *fs.FileBlob)

	go func(ch chan *fs.FileBlob) {
		filepath.Walk(os.Args[1], func(path string, info os.FileInfo, err error) error {
			//fmt.Printf("file: %s, error: %v\n", path, err)
			if err != nil {
				fmt.Printf("prevent panic by handling failure accessing a path %q: %v\n", path, err)
				return err
			}
			if !info.IsDir() {
				fb := fs.NewFileBlob(path)
				fb.Load()
				ch <- fb
			}
			return nil
		})
		close(ch)
	}(inCh)

	lg.Info().Msg("reading output ...")
	for bm := range fs.DetectMimeType("/tmp", 100, inCh, lg) {
		fmt.Printf("%v\n", bm)
	}
}
