package main

import (
	"log"
	"net/http"
	"time"
)

type Repeater struct {
	requests chan *Request
	forwarder http.Handler
}

func NewRepeater(forwarder http.Handler) *Repeater {
	return &Repeater{requests: make(chan *Request, 10), forwarder: forwarder}
}

func (r *Repeater) Add(request *Request) {
	r.requests <- request
}

func (r *Repeater) RepeateLoop() {
	for request := range r.requests{
		r.repeateRequest(request)
		time.Sleep(1 * time.Second)
	}
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