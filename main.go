package main

import (
	"github.com/facebookgo/httpdown"
	"github.com/jessevdk/go-flags"
	"github.com/vulcand/oxy/forward"
	"github.com/vulcand/oxy/roundrobin"
	"github.com/vulcand/oxy/testutils"
	"github.com/vulcand/oxy/utils"
	"log"
	"net/http"
	"reflect"
	"time"
)

type Config struct {
	Upstreams []string `short:"u" long:"upstream" required:"true"`
	Address   string   `short:"a" long:"address" required:"true"`
	Storage   string   `short:"s" long:"storage" default:"./storage"`
}

func ParseArgs() (Config, error) {
	config := Config{}
	_, err := flags.Parse(&config)
	return config, err
}

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

/////////////////////////////////////////////////
func main() {
	config, err := ParseArgs()
	if err != nil {
		log.Fatalf("Cannot parse arguments")
	}
	forwarder, err := forward.New(forward.ErrorHandler(utils.ErrorHandlerFunc(ErrorHandler)))
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

	repeater, err := NewRepeater(loadBalancer, config.Storage)
	if err != nil {
		log.Fatalf("cannot create repeater")
	}

	streamer := NewStreamer(loadBalancer, repeater)

	server := &http.Server{
		Addr:    config.Address,
		Handler: streamer,
	}

	// Прикруть graceful shutdown, которая бы останавливала прием/отправку запросов, а все запросы,
	// которые оказались в процессе обработки сохранить на диск в рабочий каталог.
	serverWrapper := &httpdown.HTTP{
		StopTimeout: 10 * time.Second,
		KillTimeout: 1 * time.Second,
	}

	if err = httpdown.ListenAndServe(server, serverWrapper); err != nil {
		log.Fatalf("Cannot start server: %v", err)
	}

	repeater.Stop()
}
