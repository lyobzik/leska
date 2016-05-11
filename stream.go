package main

import (
	"log"
	"net/http"
)

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

	if 500 <= outResponse.code && outResponse.code < 600 {
		statusCode := http.StatusAccepted
		response.WriteHeader(statusCode)
		response.Write([]byte(http.StatusText(statusCode)))
		return
	}
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
