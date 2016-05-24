package main

import (
	"github.com/mailgun/multibuf"
	"github.com/pkg/errors"
	"github.com/vulcand/oxy/utils"
	"io"
	"net/http"
)

type Response struct {
	header http.Header
	buffer multibuf.WriterOnce
	code   int
}

func NewResponse() (*Response, error) {
	buffer, err := multibuf.NewWriterOnce()
	if err != nil {
		return nil, errors.Wrap(err, "cannot create response buffer")
	}

	return &Response{
		header: make(http.Header),
		buffer: buffer,
		code:   0,
	}, nil
}

func (r *Response) Close() {
	r.buffer.Close()
}

func (r *Response) Copy(response http.ResponseWriter) error {
	reader, err := r.buffer.Reader()
	if err != nil {
		return errors.Wrap(err, "cannot read response body")
	}
	utils.CopyHeaders(response.Header(), r.Header())
	response.WriteHeader(r.code)
	io.Copy(response, reader)
	return nil
}

// Implement http.ResponseWriter interface.
func (r *Response) Header() http.Header {
	return r.header
}

func (r *Response) Write(data []byte) (int, error) {
	return r.buffer.Write(data)
}

func (r *Response) WriteHeader(code int) {
	r.code = code
}
