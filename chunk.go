package main

import (
	"path"
	"sync"
	"time"
	"os"
	"io/ioutil"
	"fmt"
	"github.com/pkg/errors"
	"path/filepath"
)

type Chunk struct {
	file *os.File
	IsEmpty bool
	path string
}

func NewChunk(path string) (*Chunk, error) {
	prefix := fmt.Sprintf("%d_", time.Now().Unix())
	file, err := ioutil.TempFile(path, prefix)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot create chunk '%s'", prefix)
	}
	return &Chunk{file: file, IsEmpty: true, path: path}, nil
}

func (c *Chunk) Store(request *Request) {
	request.Save(c.file)
	c.IsEmpty = false
}

func (c *Chunk) Finalize(resultDir string) error {
	c.file.Close()
	err := os.Rename(c.file.Name(), path.Join(resultDir, path.Base(c.file.Name())))
	if err != nil {
		return errors.Wrapf(err, "cannot finalize chunk '%s'", c.file.Name())
	}
	return nil
}

//////////////////////
type LoadedChunk struct {
	file *os.File
}

func LoadChunk(path string) (*LoadedChunk, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.Wrap(err, "cannot open request file")
	}
	return &LoadedChunk{file: file}, nil
}

func LoadAvailableChunk(path string) (*LoadedChunk, error) {
	files, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot get list of files in '%s'", path)
	}
	if len(files) == 0 {
		return nil, nil
	}
	chunk, err := LoadChunk(filepath.Join(path, files[0].Name()))
	if err != nil {
		return nil, errors.Wrapf(err, "cannot load chunk")
	}
	return chunk, err
}

func (c *LoadedChunk) GetRequest() (*Request, error) {
	return loadRequest(c.file)
}

func (c *LoadedChunk) Close() {
	c.file.Close()
	os.Remove(c.file.Name())
}

//////////////////////
type Storer struct {
	storageDir string
	tmpDir     string
	requests chan *Request
	waiter sync.WaitGroup
	currentChunk *Chunk
}

func NewStorer(storage string) (*Storer, error) {
	storageDir := path.Join(storage, "storage")
	tmpDir := path.Join(storage, "tmp")
	if err := EnsureDirs(storageDir, tmpDir); err != nil {
		return nil, err
	}
	currentChunk, err := NewChunk(tmpDir)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create chunk")
	}

	return &Storer{storageDir: storageDir,
		tmpDir: tmpDir,
		requests: make(chan *Request, 100000),
		currentChunk: currentChunk,
	}, nil
}

func (s *Storer) Add(request *Request) {
	s.requests <- request
}

func (s *Storer) Spawn() {
	s.waiter.Add(1)
	go s.StoreLoop()
}

func (s *Storer) Stop() {
	close(s.requests)
	s.waiter.Wait()
}

func (s *Storer) StoreLoop() {
	timer := time.Tick(5 * time.Second)
Loop:
	for {
		select {
		case request, received := <- s.requests:
			if !received {
				break Loop
			}
			s.currentChunk.Store(request)
			request.Close()
		case <-timer:
			if !s.currentChunk.IsEmpty {
				s.currentChunk.Finalize(s.storageDir)
				currentChunk, err := NewChunk(s.tmpDir)
				if err != nil {
					// Write error to log and finish service
					break Loop
				}
				s.currentChunk = currentChunk
			}
		}
	}
	s.waiter.Done()
}