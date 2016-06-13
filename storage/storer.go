package storage

import (
	"github.com/op/go-logging"
	"github.com/pkg/errors"
	"github.com/lyobzik/go-utils"
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
	stopper    *utils.Stopper
}

func NewStorer(logger *logging.Logger, storage string) (*Storer, error) {
	storageDir := path.Join(storage, "storage")
	tmpDir := path.Join(storage, "tmp")
	if err := utils.EnsureDirs(storageDir, tmpDir); err != nil {
		return nil, errors.Wrap(err, "cannot create storage directory")
	}

	return &Storer{logger: logger,
		storageDir: storageDir,
		tmpDir:     tmpDir,
		data:       make(chan Data, 100000),
		Chunks:     make(chan string, 100000),
		stopper:    utils.NewStopper(),
	}, nil
}

func StartStorer(logger *logging.Logger, storage string) (*Storer, error) {
	storer, err := NewStorer(logger, storage)
	if err == nil {
		storer.Spawn()
	}
	return storer, err
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

	finalizedChunks, err := utils.GetFiles(s.storageDir)
	if err != nil {
		s.logger.Errorf("cannot read inialized chunk list: %v", err)
		return
	}
	s.logger.Infof("finalized chunks on startup: %v", finalizedChunks)

	currentChunk, err := NewChunk(s.tmpDir)
	// TODO: нужно закрывать currentChunk, но при этом не закрывать его дважды. То есть простой defer не поможет.
	if err != nil {
		s.logger.Errorf("cannot create chunk: %v", err)
		return
	}
	defer func() {
		s.finalizeChunk(currentChunk)
	}()
	// TODO: нужно задавать время жизни чанка в конфиге
	timer := time.Tick(5 * time.Second)

	mayRun := true
	for mayRun {
		if len(finalizedChunks) == 0 {
			select {
			case data, received := <-s.data:
				mayRun = s.handleData(currentChunk, data, received)
			case <-timer:
				var name string
				mayRun, name, currentChunk = s.finalizeChunk(currentChunk)
				if mayRun && len(name) != 0 {
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
				if mayRun && len(name) != 0 {
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
	if err := chunk.Store(data); err != nil {
		s.logger.Errorf("cannot store data to chunk: %v", err)
	}
	data.Close()
	return true
}

func (s *Storer) finalizeChunk(chunk *WriteChunk) (bool, string, *WriteChunk) {
	if !chunk.IsEmpty {
		finalizedPath, err := chunk.Finalize(s.storageDir)
		s.logger.Infof("finializeChunk %v: %s", chunk, finalizedPath)
		if err != nil {
			s.logger.Errorf("cannot finalize chunk: %v", err)
			return false, "", nil
		}
		newChunk, err := NewChunk(s.tmpDir)
		if err != nil {
			s.logger.Errorf("cannot create new chunk: %v", err)
			return false, "", nil
		}
		return true, finalizedPath, newChunk
	}
	return true, "", chunk
}
