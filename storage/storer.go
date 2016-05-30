package storage

import (
	"github.com/op/go-logging"
	"github.com/pkg/errors"
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
	go s.storeLoop()
}

func (s *Storer) Stop() {
	close(s.data)
	s.stopper.Stop()
	s.stopper.WaitDone()
}

func (s *Storer) storeLoop() {
	defer s.stopper.Done()

	finalizedChunks, err := GetFiles(s.storageDir)
	if err != nil {
		s.logger.Errorf("cannot read inialized chunk list: %v", err)
		return
	}

	currentChunk, err := NewChunk(s.tmpDir)
	// TODO: нужно закрывать currentChunk, но при этом не закрывать его дважды. То есть простой defer не поможет.
	if err != nil {
		s.logger.Errorf("cannot create chunk: %v", err)
		return
	}
	defer func() {
		currentChunk.Finalize(s.storageDir)
	}()
	// TODO: нужно задавать время жизни чанка в конфиге
	timer := time.Tick(5 * time.Second)

	mayRun := false
	for mayRun {
		if len(finalizedChunks) == 0 {
			select {
			case data, received := <-s.data:
				mayRun = s.handleData(currentChunk, data, received)
			case <-timer:
				var name string
				mayRun, name, currentChunk = s.finalizeChunk(currentChunk)
				if mayRun && len(name) == 0 {
					finalizedChunks = append(finalizedChunks, name)
				}
			}
		} else {
			select {
			case data, received := <-s.data:
				mayRun = s.handleData(currentChunk, data, received)
			case <-timer:
				var name string
				mayRun, name, currentChunk = s.finalizeChunk(currentChunk)
				if mayRun && len(name) == 0 {
					finalizedChunks = append(finalizedChunks, name)
				}
			case s.Chunks <- finalizedChunks[0]:
				finalizedChunks = finalizedChunks[1:]
			}
		}
	}
}

func (s *Storer) handleData(chunk *WriteChunk, data Data, received bool) bool {
	if !received {
		return false
	}
	chunk.Store(data)
	data.Close()
	return true
}

func (s *Storer) finalizeChunk(chunk *WriteChunk) (bool, string, *WriteChunk) {
	if !chunk.IsEmpty {
		err := chunk.Finalize(s.storageDir)
		if err != nil {
			s.logger.Errorf("cannot finalize chunk: %v", err)
			return false, "", nil
		}
		newChunk, err := NewChunk(s.tmpDir)
		if err != nil {
			s.logger.Errorf("cannot create new chunk: %v", err)
			return false, "", nil
		}
		return true, chunk.name(), newChunk
	}
	return true, "", chunk
}