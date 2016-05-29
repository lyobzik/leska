package storage

import (
	"github.com/howeyc/fsnotify"
	"github.com/op/go-logging"
	"github.com/pkg/errors"
	"io/ioutil"
	"log"
	"path"
	"path/filepath"
	"time"
)

type Storer struct {
	logger     *logging.Logger
	storageDir string
	tmpDir     string
	data       chan Data
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
		data:       make(chan Data, 100000),
		Chunks:     make(chan string, 100000),
		stopper:    NewStopper(),
	}, nil
}

func (s *Storer) Add(data Data) {
	s.data <- data
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
	close(s.data)
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
		case data, received := <-s.data:
			if !received {
				break Loop
			}
			currentChunk.Store(data)
			data.Close()
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
		case <-s.stopper.Stopping:
			return
		}
	}
}
