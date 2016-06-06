package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"
	"bytes"
)

// TODO: завернуть все ошибки через errors.Wrap с нормальной формулировкой.
// Chunk data.
type Data interface {
	Close()
	Save(io.Writer) error
}

type RecordHeader struct {
	TTL  int32
	Size uint64
}

func (r *RecordHeader) Write(writer io.Writer) error {
	for _, value := range []interface{}{r.TTL, r.Size} {
		if err := binary.Write(writer, binary.BigEndian, value); err != nil {
			return err
		}
	}
	return nil
}

func (r *RecordHeader) Read(reader io.Reader) error {
	for _, value := range []interface{}{&r.TTL, &r.Size} {
		if err := binary.Read(reader, binary.BigEndian, value); err != nil {
			return err
		}
	}
	return nil
}


type Record struct {
	RecordHeader
	Data Data
	Reader *bufio.Reader
}

func NewRecord(data Data) *Record {
	return &Record{Data: data}
}

func (r *Record) Close() {
	if r.Data != nil {
		r.Data.Close()
	}
}

func (r *Record) Save(writer io.Writer) error {
	// Понятно, что здесь зря дублируется запись данных, но пока не понятно как иначе узнать
	// размер записанных данных для заголовка. При использовании mmapped-файлов эта проблема уйдет.
	var buffer bytes.Buffer
	if err := r.Data.Save(&buffer); err != nil {
		return err
	}
	r.Size = uint64(buffer.Len())
	if err := r.RecordHeader.Write(writer); err != nil {
		return err
	}
	// TODO: правильно обрабатывать если удалось записать не весь buffer (в некоторых случаях повторить, в некоторых сломаться)
	if _, err := writer.Write(buffer.Bytes()); err != nil {
		return err
	}
	return nil
}

func (r *Record) SetReader(reader io.Reader) {
	r.Reader = bufio.NewReader(io.LimitReader(reader, int64(r.Size)))
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
type ChunkHeader struct {
	MagicNumber uint32
	Version     uint32
	Length      uint64
}

type RecordInfo struct {
	RecordHeader
	Offset int64
	Size   uint32
}

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

func (c *ReadChunk) GetNextRecordReader() (*Record, error) {
	record := NewRecord(nil)
	// TODO: цикл по true это плохо, нужно заменить его на нормальное условие
	for record.Reader == nil {
		if err := record.Read(c.Reader); err != nil {
			return nil, err
		}
		// TODO: нужно подумать как лучше считать TTL, по хорошему записи с нулевым ТТЛ лучше вообще в файл не сохранять.
		// но на случай если у нас уже будет mmapped-файл мы же не сможем их оттуда удалять, поэтому пока такую обработку
		// оставим.
		if record.TTL > 0 {
			record.SetReader(c.Reader)
		} else {
			c.Reader.Discard(int(record.Size))
		}
	}
	return record, nil
}

func (c *ReadChunk) Name() string {
	return c.file.Name()
}

func (c *ReadChunk) Close() {
	c.file.Close()
	os.Remove(c.file.Name())
}
