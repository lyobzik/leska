package main

import (
	"github.com/op/go-logging"
	"net/http"
	"os"
	"sync"
	"time"
)

type Repeater struct {
	logger      *logging.Logger
	forwarder   http.Handler
	waiter      sync.WaitGroup
	storagePath string
	stopping    chan struct{}
	storer      *Storer
}

func NewRepeater(logger *logging.Logger, forwarder http.Handler, storage string) (*Repeater, error) {
	err := os.MkdirAll(storage, os.ModeDir|0777)
	if err != nil {
		return nil, err
	}

	storer, err := NewStorer(storage)
	if err != nil {
		return nil, err
	}
	storer.Spawn()

	repeater := &Repeater{
		logger:      logger,
		forwarder:   forwarder,
		storagePath: storage,
		stopping:    make(chan struct{}, 1),
		storer:      storer,
	}

	repeater.waiter.Add(1)
	go repeater.RepeateLoop()
	return repeater, nil
}

func (r *Repeater) Stop() {
	// TODO: Прикруть graceful shutdown, которая бы останавливала прием/отправку запросов, а все запросы,
	// которые оказались в процессе обработки сохранить на диск в рабочий каталог.

	close(r.stopping)
	r.waiter.Wait()
}

func (r *Repeater) Add(request *Request) {
	r.storer.Add(request)
}

func (r *Repeater) RepeateLoop() {
	var currentChunk *LoadedChunk
	for {
		select {
		case <-r.stopping:
			r.logger.Info("receive stopping signal")
			r.waiter.Done()
			return
		default:
			// TODO: нужно добавить inotify для ослеживания содержимого каталог
			if currentChunk == nil {
				var err error
				currentChunk, err = LoadAvailableChunk(r.storer.storageDir)
				if err != nil {
					time.Sleep(1 * time.Second)
					r.logger.Error("cannot get chunk")
				}
			}
			if currentChunk != nil {
				err := r.repeateChunkRequest(currentChunk)
				r.logger.Errorf("RepeateChunkRequest result: %v", err)
				if err != nil {
					currentChunk.Close()
					currentChunk = nil
				}
				time.Sleep(10 * time.Second)
			}
		}
	}
}

func (r *Repeater) repeateChunkRequest(chunk *LoadedChunk) error {
	request, err := chunk.GetRequest()
	if err != nil {
		return err
	}
	defer request.Close()
	r.repeateRequest(request)

	return nil
}

func (r *Repeater) repeateRequest(request *Request) {
	r.logger.Infof("repeate request: %v", request)
	response, err := NewResponse()
	if err != nil {
		return
	}
	defer response.Close()

	r.forwarder.ServeHTTP(response, &request.httpRequest)
	if 500 <= response.code && response.code < 600 {
		r.Add(request)
		return
	}
	request.Close()
}
