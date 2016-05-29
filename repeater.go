package main

import (
	"github.com/op/go-logging"
	"github.com/pkg/errors"
	"io"
	"net/http"
	"sync"
)

type Repeater struct {
	logger   *logging.Logger
	handler  http.Handler
	storer   *Storer
	waitDone sync.WaitGroup
	stopping chan struct{}
}

func NewRepeater(logger *logging.Logger, handler http.Handler, storage string) (*Repeater, error) {
	storer, err := NewStorer(storage)
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
		case chunk, received := <-r.storer.ChunkFiles:
			if !received {
				return
			}
			r.repeateChunk(chunk)
		}
	}
}

func (r *Repeater) repeateChunk(chunkName string) {
	chunk, err := r.storer.LoadChunk(chunkName)
	if err != nil {
		r.logger.Errorf("cannot load chunk from '%s': %v", chunkName, err)
		return
	}
	chunk.Close()

	//for {
	//	request, err := chunk.GetRequest()
	//	// TODO: доделать, так как err это может быть обернутый io.EOF
	//	if err == io.EOF {
	//		return
	//	} else if err != nil {
	//		r.logger.Errorf("cannot load request from chunk '%s': %v", chunk, err)
	//	}
	//
	//	r.repeateRequest(request)
	//	// Закрываем явно, чтобы бысрее освобождалась память.
	//	request.Close()
	//}

	for r.repeateChunkRequest(chunk) {}

}

func (r *Repeater) repeateChunkRequest(chunk *LoadedChunk) bool {
	if request, err := chunk.GetRequest(); err == nil {
		defer request.Close()
		r.repeateRequest(request)
	} else if err == io.EOF {
		// TODO: доделать, так как err это может быть обернутый io.EOF
		return false
	} else if err != nil {
		r.logger.Errorf("cannot load request from chunk '%s': %v", chunk, err)
	}
	return true
}

func (r *Repeater) getRequest(chuckName string, chunk *LoadedChunk) *Request {
	for {
		if request, err := chunk.GetRequest(); err == nil {
			return request
		} else if err == io.EOF {
			// TODO: доделать, так как err это может быть обернутый io.EOF
			return nil
		} else {
			r.logger.Errorf("cannot load request from chunk '%s': %v", chuckName, err)
		}
	}
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
	}
}
