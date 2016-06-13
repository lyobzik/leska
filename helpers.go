package main

import (
	"net/http"
	"net/url"
	"os"

	"github.com/op/go-logging"
	"github.com/pkg/errors"
	"github.com/vulcand/oxy/forward"
	"github.com/vulcand/oxy/roundrobin"
)

func CreateLogger(level logging.Level, prefix string) (*logging.Logger, error) {
	logger, err := logging.GetLogger(prefix)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot create logger")
	}

	backend := logging.NewLogBackend(os.Stderr, prefix, 0)

	format := " %{color}%{time:15:04:05.000} [%{level}]%{color:reset} %{message}"
	formatter := logging.MustStringFormatter(format)
	formattedBackend := logging.NewBackendFormatter(backend, formatter)

	leveledBackend := logging.AddModuleLevel(formattedBackend)
	leveledBackend.SetLevel(level, "")

	logger.SetBackend(leveledBackend)
	return logger, nil
}

func CreateForwarder(logger *logging.Logger, upstreams []string) (http.Handler, error) {
	forwarder, err := forward.New(forward.Logger(logger))
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
