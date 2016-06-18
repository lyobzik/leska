package storage

import (
	"github.com/edsrzf/mmap-go"
	"github.com/pkg/errors"
	"os"
	"reflect"
	"time"
	"unsafe"
)

const (
	indexMagic   = 0x0001e5ca
	indexVersion = 1
)

// TODO: добавлять заголовок в начало файла
type IndexHeader struct {
	MagicNumber int32
	Version     int32
	Lenght      int64 // in elements number
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
	index.Header.MagicNumber = indexMagic
	index.Header.Version = indexVersion
	index.Header.Lenght = 0
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
	if index.Header.MagicNumber != indexMagic {
		return nil, errors.New("incorrect magic number of index file")
	}
	if index.Header.Version != indexVersion {
		return nil, errors.New("unsupperted version of index file")
	}

	index.recordsInfo = (*reflect.SliceHeader)(unsafe.Pointer(&index.Records))
	index.recordsInfo.Data = uintptr(unsafe.Pointer(&data[0])) + unsafe.Sizeof(IndexHeader{})
	index.recordsInfo.Len = int(index.Header.Lenght)

	return index, nil

}

func (index *Index) AppendRecord() (int, error) {
	stat, err := index.file.Stat()
	if err != nil {
		return -1, errors.Wrapf(err, "cannot get initiale file size")
	}
	err = index.file.Truncate(stat.Size() + int64(unsafe.Sizeof(IndexRecord{})))
	if err != nil {
		return -1, errors.Wrapf(err, "cannot resize file")
	}

	index.Header.Lenght += 1
	index.Header.ActiveCount += 1

	index.recordsInfo.Len = int(index.Header.Lenght)
	return int(index.Header.Lenght - 1), nil
}

func (index *Index) Close() error {
	index.recordsInfo.Data = 0
	index.recordsInfo.Len = 0
	index.Header = nil
	index.recordsInfo = nil

	if err := index.data.Unmap(); err != nil {
		return errors.Wrapf(err, "cannot unmap file")
	}
	if err := index.file.Close(); err != nil {
		return errors.Wrapf(err, "cannot close file")
	}

	return nil
}
