package main

import (
	"fmt"
	"github.com/howeyc/fsnotify"
	"github.com/op/go-logging"
	"github.com/pkg/errors"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"time"
)

type WriteChunk struct {
	file    *os.File
	IsEmpty bool
}

func NewChunk(path string) (*WriteChunk, error) {
	prefix := fmt.Sprintf("%d_", time.Now().Unix())
	file, err := ioutil.TempFile(path, prefix)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot create chunk '%s'", prefix)
	}
	return &WriteChunk{file: file, IsEmpty: true}, nil
}

func (c *WriteChunk) Store(request *Request) error {
	if err := request.Save(c.file); err != nil {
		return errors.Wrapf(err, "cannot save request to chunk '%s'", c.name())
	}
	c.IsEmpty = false
	return nil
}

func (c *WriteChunk) Finalize(path string) error {
	if err := c.file.Close(); err != nil {
		return errors.Wrapf(err, "cannot close chunk '%s'", c.name())
	}
	if err := os.Rename(c.file.Name(), filepath.Join(path, c.name())); err != nil {
		return errors.Wrapf(err, "cannot move chunk '%s' to storage '%s'", c.name(), path)
	}
	return nil
}

func (c *WriteChunk) name() string {
	return c.file.Name()
}

//////////////////////
type ReadChunk struct {
	file *os.File
}

func LoadChunk(path string) (*ReadChunk, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.Wrap(err, "cannot open chunk file")
	}
	return &ReadChunk{file: file}, nil
}

func (c *ReadChunk) Name() string {
	return c.file.Name()
}

func (c *ReadChunk) GetRequest() (*Request, error) {
	return LoadRequest(c.file, 1024*1024)
}

func (c *ReadChunk) Close() {
	c.file.Close()
	os.Remove(c.file.Name())
}

//////////////////////
type Storer struct {
	logger     *logging.Logger
	storageDir string
	tmpDir     string
	requests   chan *Request
	Chunks     chan string
	stopper    *Stopper
}

func NewStorer(logger *logging.Logger, storage string) (*Storer, error) {
	storageDir := path.Join(storage, "storage")
	tmpDir := path.Join(storage, "tmp")
	if err := EnsureDirs(storageDir, tmpDir); err != nil {
		return nil, errors.Wrap(err, "cannot create storage directory")
	}

	return &Storer{logger: logger,
		storageDir: storageDir,
		tmpDir:     tmpDir,
		requests:   make(chan *Request, 100000),
		Chunks:     make(chan string, 100000),
		stopper:    NewStopper(),
	}, nil
}

func (s *Storer) Add(request *Request) {
	s.requests <- request
}

func (s *Storer) LoadChunk(chunkName string) (*ReadChunk, error) {
	return LoadChunk(filepath.Join(s.storageDir, chunkName))
}

func (s *Storer) Spawn() {
	s.stopper.Add()
	go s.StoreLoop()
	s.stopper.Add()
	go s.ChunksWatch()
}

func (s *Storer) Stop() {
	close(s.requests)
	s.stopper.Stop()
	s.stopper.WaitDone()
}

func (s *Storer) StoreLoop() {
	defer s.stopper.Done()
	currentChunk, err := NewChunk(s.tmpDir)
	if err != nil {
		s.logger.Errorf("cannot create chunk: %v", err)
		return
	}
	timer := time.Tick(5 * time.Second)
Loop:
	for {
		select {
		case request, received := <-s.requests:
			if !received {
				break Loop
			}
			currentChunk.Store(request)
			request.Close()
		case <-timer:
			if !currentChunk.IsEmpty {
				currentChunk.Finalize(s.storageDir)
				var err error
				currentChunk, err = NewChunk(s.tmpDir)
				if err != nil {
					// Write error to log and finish service
					break Loop
				}
			}
		}
	}
}

func (s *Storer) ChunksWatch() {
	defer s.stopper.Done()
	defer close(s.Chunks)
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatalf("cannot create storage notifier: %v", err)
	}
	defer watcher.Close()
	watcher.WatchFlags(s.storageDir, fsnotify.FSN_CREATE)

	files, err := ioutil.ReadDir(s.storageDir)
	if err != nil {
		log.Fatalf("cannot get list of files in '%s'", s.storageDir)
	}
	for _, file := range files {
		s.Chunks <- file.Name()
	}

	for {
		select {
		case watchError, received := <-watcher.Error:
			if !received {
				return
			}
			log.Fatalf("watching error: %v", watchError)
		case watchEvent, received := <-watcher.Event:
			if !received {
				return
			}
			if watchEvent.IsCreate() {
				s.Chunks <- watchEvent.Name
			}
		case <- s.stopper.Stopping:
			return
		}
	}
}
