package storage

import (
	"os"
	"github.com/pkg/errors"
	"sync"
	"io/ioutil"
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

//
type Stopper struct {
	waitDone   sync.WaitGroup
	Stopping   chan struct{}
}

func NewStopper() *Stopper {
	return &Stopper{
		waitDone: sync.WaitGroup{},
		Stopping: make(chan struct{}, 1),
	}
}

func (s *Stopper) Stop() {
	close(s.Stopping)
}

func (s *Stopper) WaitDone() {
	s.waitDone.Wait()
}

func (s *Stopper) Add() {
	s.waitDone.Add(1)
}

func (s *Stopper) Done() {
	s.waitDone.Done()
}