package storage

import (
	"io"
	"strings"
	"time"

	"github.com/lyobzik/go-utils"
	"github.com/op/go-logging"
	"github.com/pkg/errors"
)

type Data interface {
	Close()
	Save(io.Writer) (int, error)
}

type DataRecord struct {
	Data    Data
	TTL     int32
	LastTry time.Time
}

type Storer struct {
	logger        *logging.Logger
	storage       string
	repeatNumber  int32
	chunkLifetime time.Duration
	data          chan DataRecord
	stopper       *utils.Stopper
	Chunks        chan string
}

func NewStorer(logger *logging.Logger, storage string, repeatNumber int32,
	chunkLifetime time.Duration, bufferSize int) (*Storer, error) {

	if err := utils.EnsureDir(storage); err != nil {
		return nil, errors.Wrap(err, "cannot create storage directory")
	}

	return &Storer{logger: logger,
		storage:       storage,
		repeatNumber:  repeatNumber,
		chunkLifetime: chunkLifetime,
		data:          make(chan DataRecord, bufferSize),
		stopper:       utils.NewStopper(),
		Chunks:        make(chan string, bufferSize),
	}, nil
}

func StartStorer(logger *logging.Logger, storage string, repeatNumber int32,
	chunkLifetime time.Duration, bufferSize int) (*Storer, error) {

	storer, err := NewStorer(logger, storage, repeatNumber, chunkLifetime, bufferSize)
	if err == nil {
		storer.Spawn()
	}
	return storer, err
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

func (s *Storer) Add(data Data) {
	s.AddWithTTL(data, s.repeatNumber)
}

func (s *Storer) AddWithTTL(data Data, ttl int32) {
	s.data <- DataRecord{Data: data, TTL: ttl, LastTry: time.Now()}
}

func (s *Storer) storeLoop() {
	finalizedChunks, err := utils.GetFilteredFiles(s.storage,
		".*"+strings.Replace(indexSuffix, ".", "\\.", -1))
	if err != nil {
		s.logger.Errorf("cannot read inialized chunk list: %v", err)
		return
	}
	s.logger.Infof("finalized chunks on startup: %v", finalizedChunks)

	chunk := s.createChunk()
	defer func() {
		s.finalizeChunk(chunk)
		s.stopper.Done()
	}()

	timer := time.Tick(s.chunkLifetime)

	mayRun := true
	for mayRun && chunk != nil {
		if len(finalizedChunks) == 0 {
			select {
			case data, received := <-s.data:
				mayRun = s.handleData(chunk, data, received)
			case <-timer:
				if chunk.Index.Header.ActiveCount > 0 {
					finalizedChunks = append(finalizedChunks, chunk.Path)
				}
				chunk = s.recreateChunk(chunk)
			}
		} else {
			select {
			case data, received := <-s.data:
				mayRun = s.handleData(chunk, data, received)
			case <-timer:
				if chunk.Index.Header.ActiveCount > 0 {
					finalizedChunks = append(finalizedChunks, chunk.Path)
				}
				chunk = s.recreateChunk(chunk)
			case s.Chunks <- finalizedChunks[0]:
				finalizedChunks = finalizedChunks[1:]
			}
		}
	}
}

func (s *Storer) handleData(chunk *Chunk, data DataRecord, received bool) bool {
	if !received {
		return false
	}
	defer data.Data.Close()
	if err := chunk.Store(data); err != nil {
		s.logger.Errorf("cannot store data to chunk: %v", err)
	}
	return true
}

func (s *Storer) recreateChunk(chunk *Chunk) *Chunk {
	if s.finalizeChunk(chunk) {
		return s.createChunk()
	}
	return nil
}

func (s *Storer) createChunk() *Chunk {
	chunk, err := CreateChunk(s.storage)
	if err != nil {
		s.logger.Errorf("cannot create new chunk: %v", err)
		return nil
	}
	return chunk
}

func (s *Storer) finalizeChunk(chunk *Chunk) bool {
	if chunk != nil {
		if err := chunk.Finalize(); err != nil {
			s.logger.Errorf("cannot finalize chunk: %v", err)
			return false
		}
	}
	return true
}
