package main

import (
	"bufio"
	"bytes"
	"net/http"
	"time"

	"github.com/lyobzik/go-utils"
	"github.com/lyobzik/leska/storage"
	"github.com/op/go-logging"
)

type Repeater struct {
	logger        *logging.Logger
	handler       http.Handler
	storer        *storage.Storer
	repeatTimeout time.Duration
	repeatNumber  int32
	stopper       *utils.Stopper
}

func NewRepeater(logger *logging.Logger, handler http.Handler, storer *storage.Storer,
	repeatTimeout time.Duration, repeatNumber int32) (*Repeater, error) {

	return &Repeater{
		logger:        logger,
		handler:       handler,
		storer:        storer,
		repeatTimeout: repeatTimeout,
		repeatNumber:  repeatNumber,
		stopper:       utils.NewStopper(),
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
	r.stopper.Add()
	go r.repeateLoop()
}

func (r *Repeater) Stop() {
	r.stopper.Stop()
	r.stopper.WaitDone()
}

func (r *Repeater) repeateLoop() {
	defer r.stopper.Done()

	for {
		select {
		case <-r.stopper.Stopping:
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
	chunk, err := storage.OpenChunk(chunkName)
	if err != nil {
		r.logger.Errorf("cannot load chunk from '%s': %v", chunkName, err)
		return
	}
	defer chunk.Close()

	chunk.ForEachActiveRecord(r.repeatTimeout, r.repeateRecord)
}

func (r *Repeater) repeateRecord(chunk *storage.Chunk, record storage.IndexRecord) bool {
	r.logger.Infof("repeate request from chunk: %v", chunk)
	requestData, err := chunk.Restore(record)
	if err != nil {
		r.logger.Error("cannot restore record from chunk: %v", err)
		return false
	}
	requestDataReader := bufio.NewReader(bytes.NewBuffer(requestData))
	request, err := LoadRequest(requestDataReader, 1024*1024)
	if err != nil {
		r.logger.Error("cannot load request: %v", err)
		return false
	}
	defer request.Close()

	return r.repeateRequest(request)
}

func (r *Repeater) repeateRequest(request *Request) bool {
	response, err := NewResponse()
	if err != nil {
		return false
	}
	defer response.Close()

	r.handler.ServeHTTP(response, &request.httpRequest)
	if response.IsFailed() {
		r.logger.Errorf("cannot repeate request: %v", request)
	} else {
		r.logger.Infof("repeate successfull: %v - %v", request, response)
	}
	return !response.IsFailed()
}
