package storage

import (
	"bufio"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"
)

// Chunk data.
type Data interface {
	Close()
	Save(io.Writer) error
}

// Chunk to write data.
type WriteChunk struct {
	file    *os.File
	closed  bool
	IsEmpty bool
}

func NewChunk(path string) (*WriteChunk, error) {
	prefix := fmt.Sprintf("%d_", time.Now().Unix())
	file, err := ioutil.TempFile(path, prefix)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot create chunk '%s'", prefix)
	}
	return &WriteChunk{file: file, closed: false, IsEmpty: true}, nil
}

func (c *WriteChunk) Store(data Data) error {
	if err := data.Save(c.file); err != nil {
		return errors.Wrapf(err, "cannot save data to chunk '%s'", c.Path())
	}
	c.IsEmpty = false
	return nil
}

func (c *WriteChunk) Finalize(path string) (string, error) {
	// TODO: по-хорошему лучше это вынесте в repeateLoop, но там это кажется будет выглядеть кривовато.
	if c.closed {
		return "", nil
	}

	c.closed = true
	if err := c.file.Close(); err != nil {
		return "", errors.Wrapf(err, "cannot close chunk '%s'", c.Path())
	}
	if err := os.Rename(c.Path(), filepath.Join(path, c.Name())); err != nil {
		return "", errors.Wrapf(err, "cannot move chunk '%s' to storage '%s'", c.Path(), path)
	}
	// TODO: нужно подумать что лучше отсюда возвращать имя файла или полный путь.
	// Нужно чтобы это было согласованно с тем как получаются на старте список finalizedChunks
	return c.Name(), nil
}

func (c *WriteChunk) Name() string {
	return filepath.Base(c.Path())
}

func (c *WriteChunk) Path() string {
	return c.file.Name()
}

// Chunk to read data.
type ReadChunk struct {
	// TODO: кажется можно обойтись без поля file, а Reader встроить.
	file   *os.File
	Reader *bufio.Reader
}

func LoadChunk(path string) (*ReadChunk, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.Wrap(err, "cannot open chunk file")
	}
	return &ReadChunk{file: file, Reader: bufio.NewReader(file)}, nil
}

func (c *ReadChunk) Name() string {
	return c.file.Name()
}

func (c *ReadChunk) Close() {
	c.file.Close()
	os.Remove(c.file.Name())
}
