package main

import (
	"github.com/pkg/errors"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Repeater struct {
	forwarder   http.Handler
	waiter      sync.WaitGroup
	storagePath string
	stopping    chan struct{}
}

func NewRepeater(forwarder http.Handler, storage string) (*Repeater, error) {
	err := os.MkdirAll(storage, os.ModeDir|0777)
	if err != nil {
		return nil, err
	}

	repeater := &Repeater{
		forwarder:   forwarder,
		storagePath: storage,
		stopping:    make(chan struct{}, 1),
	}

	repeater.waiter.Add(1)
	go repeater.RepeateLoop()
	return repeater, nil
}

func (r *Repeater) Stop() {
	close(r.stopping)
	r.waiter.Wait()
}

func (r *Repeater) Add(request *Request) error {
	// Нужно придумать каким образом создавать файл атомарно, чтобы в repeateRequests мы не трогали файлы, которые еще полностью не записаны.
	file, err := ioutil.TempFile(r.storagePath, "leska")
	if err != nil {
		return errors.Wrap(err, "cannot add request to repeater")
	}
	defer file.Close()
	request.Save(file)
	return nil
}

func (r *Repeater) RepeateLoop() {
	for {
		select {
		case <-r.stopping:
			log.Printf("receive stopping signal")
			r.waiter.Done()
			return
		default:
			r.repeateRequests()
		}
	}
}

func (r *Repeater) repeateRequests() {
	files, err := ioutil.ReadDir(r.storagePath)
	if err != nil {
		log.Printf("cannot get list of files in '%s': %v", r.storagePath, err)
	}
	for _, file := range files {
		r.repeateFileRequest(file.Name())
		time.Sleep(1 * time.Second)
	}
}

func (r *Repeater) repeateFileRequest(fileName string) {
	filePath := filepath.Join(r.storagePath, fileName)
	request, err := LoadRequest(filePath)
	if err != nil {
		log.Printf("cannot load request: %v", err)
		return
	}
	defer request.Close()
	r.repeateRequest(request)
	os.Remove(filePath)
}

func (r *Repeater) repeateRequest(request *Request) {
	log.Printf("Repeate request: %v", request)
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
