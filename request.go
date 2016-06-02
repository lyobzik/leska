package main

import (
	"bufio"
	"github.com/mailgun/multibuf"
	"github.com/pkg/errors"
	"github.com/vulcand/oxy/utils"
	"io"
	"io/ioutil"
	"net/http"
)

const (
	unlimiedSize = -1
)

type Request struct {
	httpRequest http.Request
	buffer      multibuf.MultiReader
}

func NewRequest(inRequest *http.Request, memoryBufferSize int64, maxSize int64) (*Request, error) {
	if inRequest.ContentLength > maxSize && maxSize > unlimiedSize {
		return nil, errors.Errorf("request exceeded size limit (%d > %d)",
			inRequest.ContentLength, maxSize)
	}
	body, err := multibuf.New(inRequest.Body, multibuf.MemBytes(memoryBufferSize))
	if err != nil {
		return nil, errors.Wrap(err, "cannot copy request body")
	}

	request := &Request{buffer: body}
	request.copyRequest(inRequest)
	return request, nil
}

func LoadRequest(reader *bufio.Reader, memoryBufferSize int64) (*Request, error) {
	request, err := http.ReadRequest(reader)
	if err != nil {
		return nil, errors.Wrap(err, "cannot load request")
	}
	return NewRequest(request, memoryBufferSize, unlimiedSize)
}

func (r *Request) Close() {
	r.buffer.Close()
}

func (r *Request) Save(file io.Writer) error {
	err := r.httpRequest.Write(file)
	if err == nil {
		_, err = r.buffer.WriteTo(file)
	}
	return err
}

func (r *Request) copyRequest(req *http.Request) {
	copyRequest(&r.httpRequest, req, r.buffer)
}

// Helpers
func copyRequest(dstRequest *http.Request, srcRequest *http.Request, buffer io.Reader) {
	*(dstRequest) = *(srcRequest)
	dstRequest.URL = utils.CopyURL(srcRequest.URL)
	dstRequest.Header = make(http.Header)
	utils.CopyHeaders(dstRequest.Header, srcRequest.Header)
	dstRequest.ContentLength = srcRequest.ContentLength

	dstRequest.TransferEncoding = []string{}
	dstRequest.Body = ioutil.NopCloser(buffer)
}
