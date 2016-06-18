package storage
//
//import (
//	"bufio"
//	"os"
//
//	"github.com/pkg/errors"
//)
//
//// TODO: завернуть все ошибки через errors.Wrap с нормальной формулировкой.
//type ReadChunk struct {
//	// TODO: кажется можно обойтись без поля file, а Reader встроить.
//	file   *os.File
//	Reader *bufio.Reader
//}
//
//func LoadChunk(path string) (*ReadChunk, error) {
//	file, err := os.Open(path)
//	if err != nil {
//		return nil, errors.Wrap(err, "cannot open chunk file")
//	}
//	return &ReadChunk{file: file, Reader: bufio.NewReader(file)}, nil
//}
//
//func (c *ReadChunk) GetNextRecordReader() (*Record, error) {
//	record := NewRecord(nil)
//	// TODO: цикл по true это плохо, нужно заменить его на нормальное условие
//	for record.Reader == nil {
//		if err := record.Read(c.Reader); err != nil {
//			return nil, err
//		}
//		// TODO: нужно подумать как лучше считать TTL, по хорошему записи с нулевым ТТЛ лучше вообще в файл не сохранять.
//		// но на случай если у нас уже будет mmapped-файл мы же не сможем их оттуда удалять, поэтому пока такую обработку
//		// оставим.
//		if record.TTL > 0 {
//			record.SetReader(c.Reader)
//		} else {
//			c.Reader.Discard(int(record.Size))
//		}
//	}
//	return record, nil
//}
//
//func (c *ReadChunk) Name() string {
//	return c.file.Name()
//}
//
//func (c *ReadChunk) Close() {
//	c.file.Close()
//	os.Remove(c.file.Name())
//}
