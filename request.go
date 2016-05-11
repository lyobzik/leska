package main

import (
	"github.com/mailgun/multibuf"
	"github.com/pkg/errors"
	"github.com/vulcand/oxy/utils"
	"io/ioutil"
	"net/http"
)

type Request struct {
	httpRequest http.Request
	buffer      multibuf.MultiReader
}

func NewRequest(request *http.Request) (*Request, error) {
	body, err := multibuf.New(request.Body)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create inner copy of request")
	}
	if body == nil {
		return nil, errors.New("cannot create inner copy of request: empty body")
	}

	bodySize, err := body.Size()
	if err != nil {
		return nil, errors.New("cannot create inner copy of request: empty body")
	}

	outRequest := &Request{buffer: body}
	outRequest.copyRequest(request, bodySize)
	return outRequest, nil
}

func (r *Request) copyRequest(req *http.Request, bodySize int64) {
	r.httpRequest = *req
	r.httpRequest.URL = utils.CopyURL(req.URL)
	r.httpRequest.Header = make(http.Header)
	utils.CopyHeaders(r.httpRequest.Header, req.Header)
	r.httpRequest.ContentLength = bodySize
	// remove TransferEncoding that could have been previously set because we have transformed the request from chunked encoding
	r.httpRequest.TransferEncoding = []string{}
	// http.Transport will close the request body on any error, we are controlling the close process ourselves, so we override the closer here
	r.httpRequest.Body = ioutil.NopCloser(r.buffer)
}

func (r *Request) Close() {
	r.buffer.Close()
}
