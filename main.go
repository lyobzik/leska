package main

import (
	"github.com/facebookgo/httpdown"
	"github.com/jessevdk/go-flags"
	"github.com/op/go-logging"
	"github.com/pkg/errors"
	"github.com/vulcand/oxy/forward"
	"github.com/vulcand/oxy/roundrobin"
	"github.com/vulcand/oxy/utils"
	"net/http"
	"time"
	"net/url"
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

func CreateForwarder(logger *logging.Logger, upstreams []string) (http.Handler, error) {
	forwarder, err := forward.New(forward.Logger(logger),
		forward.ErrorHandler(utils.ErrorHandlerFunc(ErrorHandler)))
	if err != nil {
		return nil, errors.Wrapf(err, "cannot create forwarder")
	}

	loadBalancer, err := roundrobin.New(forwarder)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot create load balancer")
	}

	for _, upstream := range upstreams {
		upstreamUrl, err := url.Parse(upstream)
		if err != nil {
			return nil, errors.Wrapf(err, "cannot parse upstream address '%s'", upstream)
		}
		loadBalancer.UpsertServer(upstreamUrl)
	}
	return loadBalancer, nil
}

/////////////////////////////////////////////////
// TODO: возможно в эту задачу хорошо подойдет fasthttp. Нужно будет посмотреть
// TODO: на сколько все станет сложнее.
func main() {
	//go RunTestServer(":8088")
	//go RunTestServer(":8089")
	//go RunTestServer(":8090")

	config, err := ParseArgs()
	// TODO: возможно нужно в этом случае выводить usage.
	HandleErrorWithoutLogger("cannot parse arguments", err)

	logger, err := CreateLogger(logging.INFO, "leska")
	HandleErrorWithoutLogger("cannot create logger", err)

	forwarder, err := CreateForwarder(logger, config.Upstreams)
	HandleError(logger, "cannot create forwarder", err)

	repeater, err := NewRepeater(logger, forwarder, config.Storage)
	HandleError(logger, "cannot create repeater", err)
	defer repeater.Stop()

	streamer := NewStreamer(logger, repeater, forwarder)

	err = httpdown.ListenAndServe(
		&http.Server{
			Addr:    config.Address,
			Handler: streamer,
		},
		&httpdown.HTTP{
			StopTimeout: 10 * time.Second,
			KillTimeout: 1 * time.Second,
		})
	HandleError(logger, "cannot start server", err)
}
