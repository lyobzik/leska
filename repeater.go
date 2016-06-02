package main

import (
	"github.com/lyobzik/leska/storage"
	"github.com/op/go-logging"
	"github.com/pkg/errors"
	"net/http"
	"sync"
)

type Repeater struct {
	logger   *logging.Logger
	handler  http.Handler
	storer   *storage.Storer
	waitDone sync.WaitGroup
	stopping chan struct{}
}

func NewRepeater(logger *logging.Logger, handler http.Handler, path string) (*Repeater, error) {
	storer, err := storage.NewStorer(logger, path)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create storer")
	}

	repeater := &Repeater{
		logger:   logger,
		handler:  handler,
		storer:   storer,
		stopping: make(chan struct{}, 1),
	}
	repeater.Start()

	return repeater, nil
}

func (r *Repeater) Start() {
	r.storer.Spawn()

	r.waitDone.Add(1)
	go r.repeateLoop()
}

func (r *Repeater) Stop() {
	close(r.stopping)
	r.waitDone.Wait()

	r.storer.Stop()
}

func (r *Repeater) Add(request *Request) {
	r.storer.Add(request)
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
	if request, err := LoadRequest(chunk.Reader, 1024*1024); err == nil {
		defer request.Close()
		r.repeateRequest(request)
	} else if IsEndOfFileError(err) {
		r.logger.Errorf("read end of chunk: %v", err)
		return false
	} else if err != nil {
		r.logger.Errorf("cannot load request from chunk '%s': %v", chunk.Name(), err)
	}
	return true
}

func (r *Repeater) repeateRequest(request *Request) {
	response, err := NewResponse()
	if err != nil {
		return
	}
	defer response.Close()

	r.handler.ServeHTTP(response, &request.httpRequest)
	if response.IsFailed() {
		r.logger.Errorf("cannot repeate request: %v", request)
		r.Add(request)
	} else {
		r.logger.Infof("repeate successfull: %v - %v", request, response)
	}
}
