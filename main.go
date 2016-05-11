package main

import (
	"github.com/jessevdk/go-flags"
	"github.com/mailgun/multibuf"
	"github.com/pkg/errors"
	"github.com/vulcand/oxy/forward"
	"github.com/vulcand/oxy/roundrobin"
	"github.com/vulcand/oxy/testutils"
	"github.com/vulcand/oxy/utils"
	"io"
	"io/ioutil"
	"log"
	"net/http"
)

type Config struct {
	Upstreams []string `short:"u" long:"upstream" required:"true"`
	Address   string   `short:"a" long:"address" required:"true"`
}

func ParseArgs() (Config, error) {
	config := Config{}
	_, err := flags.Parse(&config)
	return config, err
}

/////////////////////////////////////////////////
type Response struct {
	header http.Header
	buffer multibuf.WriterOnce
	code   int
}

func NewResponse() (*Response, error) {
	buffer, err := multibuf.NewWriterOnce()
	if err != nil {
		return nil, errors.Wrap(err, "Cannot create response buffer")
	}

	return &Response{
		header: make(http.Header),
		buffer: buffer,
	}, nil
}

func (r *Response) Header() http.Header {
	return r.header
}

func (r *Response) Write(data []byte) (int, error) {
	return r.buffer.Write(data)
}

func (r *Response) WriteHeader(code int) {
	r.code = code
}

func (r *Response) Close() {
	r.buffer.Close()
}

func (r *Response) Copy(response http.ResponseWriter) (error){
	reader, err := r.buffer.Reader()
	if err != nil {
		return errors.Wrap(err, "Cannot copy response")
	}
	utils.CopyHeaders(response.Header(), r.Header())
	response.WriteHeader(r.code)
	io.Copy(response, reader)
	return nil
}

/////////////////////////////////////////////////
type Request struct {
	httpRequest http.Request
	buffer multibuf.MultiReader
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

/////////////////////////////////////////////////
type Streamer struct {
	handler http.Handler
}

func NewStreamer(handler http.Handler) *Streamer {
	return &Streamer{handler: handler}
}

func (s *Streamer) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	outRequest, err := NewRequest(request)
	if err != nil {
		// Write error to log
		s.responseError(response)
		return
	}
	defer outRequest.Close()

	outResponse, err := NewResponse()
	if err != nil {
		// Write error to log
		s.responseError(response)
		return
	}
	defer outResponse.Close()
	// Тут нужно создать подставной объект, в который будет записан ответ.
	// Из него потом прочитать ответ, если его нет (не доступились к серверу), то нужно сохранить запрос в файл.

	// На случай ошибки нужен свой ErrorHandler, который помимо формирования правильной ошибки будет
	// выставлять какой-то флаг в outRequest, по которому мы сможем понять, что его нужно сохранить и отправть повтороно.
	s.handler.ServeHTTP(outResponse, &outRequest.httpRequest)
	if err := outResponse.Copy(response); err != nil {
		// Write error to log
		s.responseError(response)
		return
	}
	log.Println(outResponse)
	log.Println(response)
}

func (s *Streamer) responseError(response http.ResponseWriter) {

}

/////////////////////////////////////////////////
func main() {
	config, err := ParseArgs()
	if err != nil {
		log.Fatalf("Cannot parse arguments")
	}
	forwarder, err := forward.New()
	if err != nil {
		log.Fatalf("Cannot create forwarder")
	}

	loadBalancer, err := roundrobin.New(forwarder)
	if err != nil {
		log.Fatalf("Cannot create load balancer")
	}

	for _, address := range config.Upstreams {
		loadBalancer.UpsertServer(testutils.ParseURI(address))
	}

	streamer := NewStreamer(loadBalancer)

	server := &http.Server{
		Addr:    config.Address,
		Handler: streamer,
	}

	err = server.ListenAndServe()
	if err != nil {
		log.Fatalf("Cannot start server: %v", err)
	}
	// Прикруть graceful shutdown, которая бы останавливала прием/отправку запросов, а все запросы,
	// которые оказались в процессе обработки сохранить на диск в рабочий каталог.
}
