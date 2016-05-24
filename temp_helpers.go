package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"reflect"
	"time"
)

const (
	HttpStatusNetworkError = 530
)

func ErrorHandler(response http.ResponseWriter, request *http.Request, err error) {
	// В целом это можно было бы и не делать, так как все равно все 500-е ошибки обрабатываются одинаково.
	statusCode := http.StatusInternalServerError
	if err != nil {
		// net.Error может быть net.Temporary и net.Timeout нужно понять что это и правильно проверять.
		// Думаю, что досылать нам нужно будет только в некоторых случаях. С другой стороны
		// 502 может отвечать nginx на сервере и в этом случае тоже нужно досылать.
		log.Printf("Error: %v - %v", reflect.TypeOf(err), err)
		statusCode = HttpStatusNetworkError
	}
	response.WriteHeader(statusCode)
	response.Write([]byte(http.StatusText(statusCode)))
}

func RunTestServer(address string) {
	http.ListenAndServe(address, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		file, err := ioutil.TempFile("tmp_server", fmt.Sprintf("%d_", time.Now().Unix()))
		if err != nil {
			log.Printf("cannot save request in tmp_server: %v", err)
		}
		defer file.Close()
		request.Write(file)
		response.WriteHeader(http.StatusOK)
	}))
}
