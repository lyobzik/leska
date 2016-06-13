package main

import (
	"github.com/lyobzik/leska/storage"
	"github.com/op/go-logging"
	"net/http"
	"sync"
	"time"
	"github.com/lyobzik/go-utils"
)

type Repeater struct {
	logger        *logging.Logger
	handler       http.Handler
	storer        *storage.Storer
	repeatTimeout time.Duration
	repeatNumber  int32
	waitDone      sync.WaitGroup
	stopping      chan struct{}
}

func NewRepeater(logger *logging.Logger, handler http.Handler, storer *storage.Storer,
	repeatTimeout time.Duration, repeatNumber int32) (*Repeater, error) {

	return &Repeater{
		logger:        logger,
		handler:       handler,
		storer:        storer,
		repeatTimeout: repeatTimeout,
		repeatNumber:  repeatNumber,
		stopping:      make(chan struct{}, 1),
	}, nil
}

func StartRepeater(logger *logging.Logger, handler http.Handler, storer *storage.Storer,
	repeatTimeout time.Duration, repeatNumber int32) (*Repeater, error) {

	repeater, err := NewRepeater(logger, handler, storer, repeatTimeout, repeatNumber)
	if err == nil {
		repeater.Start()
	}
	return repeater, err
}

func (r *Repeater) Start() {
	r.waitDone.Add(1)
	go r.repeateLoop()
}

func (r *Repeater) Stop() {
	close(r.stopping)
	r.waitDone.Wait()
}

func (r *Repeater) AddWithTTL(request *Request, ttl int32) {
	record := storage.NewRecord(request)
	record.TTL = ttl
	r.storer.Add(record)
}

func (r *Repeater) Add(request *Request) {
	record := storage.NewRecord(request)
	record.TTL = r.repeatNumber
	r.AddRecord(record)
}

func (r *Repeater) AddRecord(record *storage.Record) {
	record.TTL -= 1
	r.storer.Add(record)
}

func (r *Repeater) repeateLoop() {
	defer r.waitDone.Done()

	for {
		select {
		case <-r.stopping:
			r.logger.Info("receive stopping signal")
			return
		case chunk, received := <-r.storer.Chunks:
			if !received {
				return
			}
			r.repeateChunk(chunk)
		}
	}
}

func (r *Repeater) repeateChunk(chunkName string) {
	r.logger.Infof("repeate chunk %s", chunkName)
	chunk, err := r.storer.LoadChunk(chunkName)
	if err != nil {
		r.logger.Errorf("cannot load chunk from '%s': %v", chunkName, err)
		return
	}
	defer chunk.Close()

	for r.repeateChunkRequest(chunk) {
	}
}

func (r *Repeater) repeateChunkRequest(chunk *storage.ReadChunk) bool {
	r.logger.Infof("repeate request from chunk: %v", chunk)
	if record, err := chunk.GetNextRecordReader(); err == nil {
		if request, err := LoadRequest(record.Reader, 1024*1024); err == nil {
			defer request.Close()
			r.repeateRequest(request, record.TTL-1)
		} else if err != nil {
			r.logger.Errorf("cannot load request from chunk '%s': %v", chunk.Name(), err)
		}
	} else if utils.IsEndOfFileError(err) {
		r.logger.Errorf("read end of chunk: %v", err)
		return false
	} else if err != nil {
		r.logger.Errorf("cannot load request from chunk '%s': %v", chunk.Name(), err)
	}
	return true
}

func (r *Repeater) repeateRequest(request *Request, ttl int32) {

	response, err := NewResponse()
	if err != nil {
		return
	}
	defer response.Close()

	r.handler.ServeHTTP(response, &request.httpRequest)
	if response.IsFailed() {
		r.logger.Errorf("cannot repeate request: %v", request)
		r.AddWithTTL(request, ttl)
	} else {
		r.logger.Infof("repeate successfull: %v - %v", request, response)
	}
}
