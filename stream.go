package main

import (
	"github.com/lyobzik/go-utils"
	"github.com/op/go-logging"
	"github.com/pkg/errors"
	"net/http"
)

type Streamer struct {
	logger   *logging.Logger
	handler  http.Handler
	repeater *Repeater
}

func NewStreamer(logger *logging.Logger, repeater *Repeater, handler http.Handler) *Streamer {
	return &Streamer{
		logger:   logger,
		handler:  handler,
		repeater: repeater,
	}
}

func (s *Streamer) ServeHTTP(inResponse http.ResponseWriter, inRequest *http.Request) {
	// TODO: возможно inRequest можно скопировать после неудачной попытке отправки.
	request, response, err := s.copyRequestResponse(inRequest)
	if err != nil {
		s.responseError(inResponse, err)
		return
	}

	repeateRequest := false
	defer func() {
		utils.CloseOnFail(repeateRequest, request)
		response.Close()
	}()

	s.handler.ServeHTTP(response, &request.httpRequest)

	if response.IsFailed() {
		s.writeResponse(inResponse, http.StatusAccepted)
		s.repeater.Add(request)
		repeateRequest = true
		return
	}
	if err := response.Copy(inResponse); err != nil {
		s.responseError(inResponse, err)
		return
	}
}

func (s *Streamer) copyRequestResponse(inRequest *http.Request) (*Request, *Response, error) {
	request, err := NewRequest(inRequest, 1024*1024, 1024*1024)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "cannot copy request")
	}

	success := false
	defer func() {
		utils.CloseOnFail(success, request)
	}()

	response, err := NewResponse()
	if err != nil {
		return nil, nil, errors.Wrapf(err, "cannot create response")
	}

	success = true
	return request, response, nil
}

func (s *Streamer) writeResponse(response http.ResponseWriter, statusCode int) {
	response.WriteHeader(statusCode)
	response.Write([]byte(http.StatusText(statusCode)))
}

func (s *Streamer) responseError(response http.ResponseWriter, err error) {
	// TODO: подумать нужно ли логировать содержимое запроса (тело может быть большим), поэтому если
	// TODO: и логировать, то только какие-то заголовки.
	s.logger.Errorf("cannot handle request: %v")
	s.writeResponse(response, http.StatusInternalServerError)
}
