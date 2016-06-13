package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

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
