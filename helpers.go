package main

import (
	"github.com/op/go-logging"
	"github.com/pkg/errors"
	"io"
	"log"
	"os"
)

// Filesystem helpers
func EnsureDir(path string) error {
	return os.MkdirAll(path, os.ModeDir|0777)
}

func EnsureDirs(paths ...string) error {
	for _, path := range paths {
		if err := EnsureDir(path); err != nil {
			return errors.Wrapf(err, "cannot create directory '%s'", path)
		}
	}
	return nil
}

// Close helpers
type Closable interface {
	Close()
}

func CloseOnFail(success bool, closable Closable) {
	if !success {
		closable.Close()
	}
}

type TryClosable interface {
	Close() error
}

func TryCloseOnFail(success bool, closable TryClosable) error {
	if !success {
		return closable.Close()
	}
	return nil
}

//
func IsEndOfFileError(err error) bool {
	return errors.Cause(err) == io.EOF
}

func HandleErrorWithoutLogger(message string, err error) {
	if err != nil {
		log.Fatalf("%s: %v\n", message, err)
	}
}

func HandleError(logger *logging.Logger, message string, err error) {
	if err != nil {
		logger.Fatalf("%s: %v\n", message, err)
	}
}

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
