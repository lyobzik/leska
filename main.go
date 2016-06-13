package main

import (
	"net/http"
	"os"
	"time"

	"github.com/facebookgo/httpdown"
	"github.com/jessevdk/go-flags"
	"github.com/lyobzik/go-utils"
	"github.com/lyobzik/leska/storage"
	"github.com/op/go-logging"
)

type Config struct {
	Upstreams     []string      `short:"u" long:"upstream" required:"true" description:"group of servers of final destination"`
	Address       string        `short:"a" long:"address" required:"true" description:"listen address of this server"`
	Storage       string        `short:"s" long:"storage" default:"storage" description:"path to directory to store failed requests"`
	RepeatTimeout time.Duration `short:"t" long:"repeat-timeout" default:"0s" description:"timeout between repeated tries"`
	RepeatNumber  int32         `short:"n" long:"repeat-number" default:"1" description:"maximum number of tries"`
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

/////////////////////////////////////////////////
// TODO: возможно в эту задачу хорошо подойдет fasthttp. Нужно будет посмотреть
// TODO: на сколько все станет сложнее.
func main() {
	//go RunTestServer(":8088")
	//go RunTestServer(":8089")
	//go RunTestServer(":8090")

	config := ParseArgs()

	logger, err := CreateLogger(config.LogLevel, "leska")
	utils.HandleErrorWithoutLogger("cannot create logger", err)
	logger.Debugf("start leska with config: %v", config)

	// TODO: прокинуть Repeate-настроки куда нужно
	forwarder, err := CreateForwarder(logger, config.Upstreams)
	utils.HandleError(logger, "cannot create forwarder", err)

	storer, err := storage.StartStorer(logger, config.Storage)
	utils.HandleError(logger, "cannot create storer", err)
	defer storer.Stop()

	repeater, err := StartRepeater(logger, forwarder, storer,
		config.RepeatTimeout, config.RepeatNumber)
	utils.HandleError(logger, "cannot create repeater", err)
	defer repeater.Stop()

	err = httpdown.ListenAndServe(
		&http.Server{
			Addr:    config.Address,
			Handler: NewStreamer(logger, repeater, forwarder),
		},
		&httpdown.HTTP{
			StopTimeout: 10 * time.Second,
			KillTimeout: 1 * time.Second,
		})
	utils.HandleError(logger, "cannot start server", err)
}
