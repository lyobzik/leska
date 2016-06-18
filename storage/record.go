package storage
//
//import (
//	"bufio"
//	"encoding/binary"
//	"io"
//	"time"
//	"bytes"
//
//	//"github.com/pkg/errors"
//)
//
//// TODO: завернуть все ошибки через errors.Wrap с нормальной формулировкой.
//
//// Definition of RecordHeader and its methods.
//type RecordHeader struct {
//	TTL  int32
//	// TODO: доделать использование времени последней попытки
//	LastTry time.Time
//	Size uint64
//}
//
//func (r *RecordHeader) Write(writer io.Writer) error {
//	for _, value := range []interface{}{r.TTL, r.Size} {
//		if err := binary.Write(writer, binary.BigEndian, value); err != nil {
//			return err
//		}
//	}
//	return nil
//}
//
//func (r *RecordHeader) Read(reader io.Reader) error {
//	for _, value := range []interface{}{&r.TTL, &r.Size} {
//		if err := binary.Read(reader, binary.BigEndian, value); err != nil {
//			return err
//		}
//	}
//	return nil
//}
//
//// Definition of Record and its methods.
//type Record struct {
//	RecordHeader
//	Data Data
//	Reader *bufio.Reader
//}
//
//func NewRecord(data Data) *Record {
//	return &Record{Data: data}
//}
//
//func (r *Record) Close() {
//	if r.Data != nil {
//		r.Data.Close()
//	}
//}
//
//func (r *Record) Save(writer io.Writer) error {
//	// Понятно, что здесь зря дублируется запись данных, но пока не понятно как иначе узнать
//	// размер записанных данных для заголовка. При использовании mmapped-файлов эта проблема уйдет.
//	var buffer bytes.Buffer
//	if err := r.Data.Save(&buffer); err != nil {
//		return err
//	}
//	r.Size = uint64(buffer.Len())
//	if err := r.RecordHeader.Write(writer); err != nil {
//		return err
//	}
//	// TODO: правильно обрабатывать если удалось записать не весь buffer (в некоторых случаях повторить, в некоторых сломаться)
//	if _, err := writer.Write(buffer.Bytes()); err != nil {
//		return err
//	}
//	return nil
//}
//
//func (r *Record) SetReader(reader io.Reader) {
//	r.Reader = bufio.NewReader(io.LimitReader(reader, int64(r.Size)))
//}
