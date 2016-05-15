package main

import (
	"os"
)

func EnsureDir(path string) error {
	return os.MkdirAll(path, os.ModeDir|0777)
}

func EnsureDirs(paths ...string) error {
	for _, path := range paths {
		if err := EnsureDir(path); err != nil {
			return err
		}
	}
	return nil
}