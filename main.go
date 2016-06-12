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
	"net/url"
	"os"
	"time"
	"github.com/lyobzik/leska/storage"
)

type Config struct {
	Upstreams     []string      `short:"u" long:"upstream" required:"true" description:"group of servers of final destination"`
	Address       string        `short:"a" long:"address" required:"true" description:"listen address of this server"`
	Storage       string        `short:"s" long:"storage" default:"storage" description:"path to directory to store failed requests"`
	RepeatTimeout time.Duration `short:"t" long:"repeat-timeout" default:"0s" description:"timeout between repeated tries"`
	RepeatNumber  uint          `short:"n" long:"repeat-number" default:"1" description:"maximum number of tries"`
	Verbose       []bool        `short:"v" long:"verbose" description:"write detailed log"`
	LogLevel      logging.Level `hidden:"true"`
}

func ParseArgs() Config {
	config := Config{}
	parser := flags.NewParser(&config, flags.Default)
	_, err := parser.Parse()
	if err != nil {
		if !isFlagsHelpError(err) {
			parser.WriteHelp(os.Stderr)
		}
		os.Exit(1)
	}
	config.LogLevel = convertVerboseToLovLevel(config.Verbose)
	return config
}

func isFlagsHelpError(err error) bool {
	flagsError, converted := err.(*flags.Error)
	return converted && flagsError.Type == flags.ErrHelp
}

func convertVerboseToLovLevel(verbose []bool) logging.Level {
	for _, value := range verbose {
		if !value {
			return logging.ERROR
		}
	}
	logLevel := logging.ERROR + logging.Level(len(verbose))
	if logging.DEBUG < logLevel {
		return logging.DEBUG
	}
	return logLevel
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

	config := ParseArgs()

	logger, err := CreateLogger(config.LogLevel, "leska")
	HandleErrorWithoutLogger("cannot create logger", err)
	logger.Debugf("start leska with config: %v", config)

	// TODO: прокинуть Repeate-настроки куда нужно
	forwarder, err := CreateForwarder(logger, config.Upstreams)
	HandleError(logger, "cannot create forwarder", err)

	storer, err := storage.StartStorer(logger, config.Storage)
	HandleError(logger, "cannot create storer", err)
	defer storer.Stop()

	repeater, err := StartRepeater(logger, forwarder, storer)
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
