package storage

import (
	"os"
	"reflect"
	"time"
	"unsafe"

	"github.com/edsrzf/mmap-go"
	"github.com/pkg/errors"
)

const (
	indexMagic   = 0x0001e5ca
	indexVersion = 1
)

type IndexHeader struct {
	Magic       int32
	Version     int32
	Length      int64 // in elements number
	ActiveCount int64
}

type IndexRecord struct {
	TTL     int32
	LastTry time.Time
	Offset  int64 // in bytes
	Size    int64 // in bytes
}

type Index struct {
	Header      *IndexHeader
	Records     []IndexRecord
	file        *os.File
	data        mmap.MMap
	recordsInfo *reflect.SliceHeader
}

func OpenIndexFile(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDWR, 0666)
}

func CreateIndex(file *os.File) (*Index, error) {
	err := file.Truncate((int64)(unsafe.Sizeof(IndexHeader{})))
	if err != nil {
		return nil, errors.Wrapf(err, "cannot resize file")
	}
	data, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot map file to memory")
	}

	index := &Index{data: data, file: file}

	index.Header = (*IndexHeader)(unsafe.Pointer(&data[0]))
	// TODO: Нужно все значения в индексе сохранять с каким-то определенным порядком байт.
	index.Header.Magic = indexMagic
	index.Header.Version = indexVersion
	index.Header.Length = 0
	index.Header.ActiveCount = 0

	index.recordsInfo = (*reflect.SliceHeader)(unsafe.Pointer(&index.Records))
	index.recordsInfo.Data = uintptr(unsafe.Pointer(&data[0])) + unsafe.Sizeof(IndexHeader{})
	index.recordsInfo.Len = 0

	return index, nil
}

func OpenIndex(file *os.File) (*Index, error) {
	data, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot map file to memory")
	}

	index := &Index{data: data, file: file}
	index.Header = (*IndexHeader)(unsafe.Pointer(&data[0]))
	if index.Header.Magic != indexMagic {
		return nil, errors.New("incorrect magic number of index file")
	}
	if index.Header.Version != indexVersion {
		return nil, errors.New("unsupperted version of index file")
	}

	index.recordsInfo = (*reflect.SliceHeader)(unsafe.Pointer(&index.Records))
	index.recordsInfo.Data = uintptr(unsafe.Pointer(&data[0])) + unsafe.Sizeof(IndexHeader{})
	index.recordsInfo.Len = int(index.Header.Length)

	return index, nil
}

func (index *Index) AppendRecord() (*IndexRecord, error) {
	stat, err := index.file.Stat()
	if err != nil {
		return nil, errors.Wrapf(err, "cannot get initiale file size")
	}
	err = index.file.Truncate(stat.Size() + int64(unsafe.Sizeof(IndexRecord{})))
	if err != nil {
		return nil, errors.Wrapf(err, "cannot resize file")
	}

	index.Header.Length += 1
	index.Header.ActiveCount += 1

	index.recordsInfo.Len = int(index.Header.Length)
	return &index.Records[index.Header.Length-1], nil
}

func (index *Index) Flush() error {
	return index.data.Flush()
}

func (index *Index) Close() error {
	index.recordsInfo.Data = 0
	index.recordsInfo.Len = 0
	index.Header = nil
	index.recordsInfo = nil

	if err := index.data.Unmap(); err != nil {
		return errors.Wrapf(err, "cannot unmap file")
	}

	return nil
}
