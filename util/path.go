package util

import (
	"os"
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
