package util

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
)

// IsPathExists returns if the path exists or error
func IsPathExists(p string) (bool, error) {
	_, err := os.Stat(p)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

func PathToUrl(path string) *url.URL {
	absPath, err := filepath.Abs(path)
	if err != nil {
		panic(err.Error())
	}
	u, err := url.Parse(fmt.Sprintf("file://%s", filepath.ToSlash(absPath)))
	if err != nil {
		panic(err.Error())
	}
	return u
}

func FileExt(s string) string {
	ext := filepath.Ext(s)
	if len(ext) > 0 && ext[0:1] == "." {
		return ext[1:]
	} else {
		return ext
	}
}
