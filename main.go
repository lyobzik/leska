package main

import (
	"github.com/jessevdk/go-flags"
	"github.com/vulcand/oxy/forward"
	"github.com/vulcand/oxy/roundrobin"
	"github.com/vulcand/oxy/testutils"
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
