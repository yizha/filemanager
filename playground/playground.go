package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

type FileMeta struct {
	fullpath string
	basename string

	fileMime string

	httpMime string

	mimetypeMime string
	mimetypeExt  string

	filetypeMime string
	filetypeExt  string
}

var fileUtilityPath string

func init() {
	path, err := exec.LookPath("file")
	if err != nil {
		panic(err.Error())
	}
	fileUtilityPath = path
}

func fileMime(fpath string) (string, error) {
	out, err := exec.Command(fileUtilityPath, "-pb", "--mime-type", fpath).Output()
	if err != nil {
		return "", err
	}
	return string(out), nil
}

func scan(dir string) {
	f, err := os.Open(dir)
	if err != nil {
		panic(err.Error())
	}
	fileInfoArray, err := f.Readdir(-1)
	f.Close()
	if err != nil {
		panic(err.Error())
	}
	if fileInfoArray != nil {
		fmt.Println("filename,file-mime,http-mime,f-mime,f-ext,m-mime,m-ext")
		for _, fi := range fileInfoArray {
			fpath := filepath.Join(dir, fi.Name())
			if fi.IsDir() {
				scan(fpath)
			} else {
				meta := &FileMeta{
					fullpath: fpath,
					basename: filepath.Base(fpath),
				}
				mime, err := fileMime(fpath)
				if err == nil {
					meta.fileMime = mime
				} else {
					meta.fileMime = fmt.Sprintf("error: %s", err.Error())
				}
				fmt.Printf("%s,%s\n", meta.basename, meta.fileMime)
			}
		}
	}
}

func main() {
	scan(os.Args[1])
}
