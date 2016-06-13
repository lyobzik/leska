package storage

import (
	"github.com/pkg/errors"
	"io/ioutil"
	"os"
)

// Filesystem helpers
func EnsureDir(path string) error {
	return os.MkdirAll(path, os.ModeDir|0777)
}

func EnsureDirs(paths ...string) error {
	for _, path := range paths {
		if err := EnsureDir(path); err != nil {
			return errors.Wrapf(err, "cannot create directory '%s'", path)
		}
	}
	return nil
}

func GetFiles(path string) ([]string, error) {
	files, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot read file list in '%s'", path)
	}

	fileNames := make([]string, 0)
	for _, file := range files {
		fileNames = append(fileNames, file.Name())
	}
	return fileNames, nil
}
