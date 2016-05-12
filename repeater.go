package main

import (
	"log"
	"net/http"
	"sync"
	"time"
)

type Repeater struct {
	// Задания на повтор нужно передавать не через канал, а через ФС.
	requests  chan *Request
	forwarder http.Handler
	waiter    sync.WaitGroup
}

func NewRepeater(forwarder http.Handler) *Repeater {
	repeater := &Repeater{requests: make(chan *Request, 10), forwarder: forwarder}
	repeater.waiter.Add(1)
	return repeater
}

func (r *Repeater) Stop() {
	close(r.requests)
	r.waiter.Wait()
}

func (r *Repeater) Add(request *Request) {
	r.requests <- request
}

func (r *Repeater) RepeateLoop() {
	for request := range r.requests {
		r.repeateRequest(request)
		time.Sleep(1 * time.Second)
	}
	r.waiter.Done()
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
