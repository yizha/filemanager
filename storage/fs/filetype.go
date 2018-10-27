package fs

import (
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
)

type FileTypeInfo struct {
	Extension       string
	MimeType        string
	MimeSubType     string
	MimeEncoding    string
	MimeDescription string
}

var (
	fileCmd string
)

func init() {
	fileCmd, err := exec.LookPath("file")
	if err != nil {
		panic(err.Error())
	}
}

func runFileDetections(inputFilePath string, arg string) (string, error) {
	var cmd *os.Command
	if arg == "" {
		cmd := exec.Command(fileCmd, "-p", "-f", inputFilePath)
	} else {
		cmd := exec.Command(fileCmd, "-p", "-f", inputFilePath, arg)
	}
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return string(out), nil
}

func detectDesc(inputFilePath string) (string, error) {
	return runFileDetections(inputFilePath, "")
}

func detectMime(inputFilePath string) (string, error) {
	return runFileDetections(inputFilePath, "--mime")
}

func DetectFileType(fileBlobs []*Blob) (map[string]*FileTypeInfo, error) {
	f, err := ioutil.TempFile("", "file-list")
	if err != nil {
		return nil, err
	}
	fpath := f.Name()
	defer os.Remove(fpath)
	path2blob := make(map[string]*Blob)
	for _, blob := range fileBlobs {
		path2hash[blob.path] = blob
		_, err = f.WriteString(fmt.Sprintf("%s\n", blob.path))
		if err != nil {
			break
		}
	}
	f.Close()
	if err != nil {
		return nil, err
	}
}
