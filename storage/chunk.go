package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/lyobzik/go-utils"
	"github.com/pkg/errors"
)

const (
	indexSuffix = ".index"
	dataSuffix  = ".data"
	tmpSuffix   = ".tmp"
)

type Chunk struct {
	Index     *Index
	indexFile *os.File
	dataFile  *os.File
	Path      string
}

func CreateChunk(storagePath string) (*Chunk, error) {
	var indexFile, dataFile *os.File
	success := false
	defer func() {
		utils.TryCloseOnFail(success, dataFile)
		utils.TryCloseOnFail(success, indexFile)
	}()

	path := filepath.Join(storagePath, fmt.Sprintf("%d", time.Now().UnixNano()))
	var err error
	if indexFile, err = os.Create(GetTmpPath(GetIndexPath(path))); err != nil {
		return nil, errors.Wrapf(err, "cannot create index file for chunk '%s'", path)
	}
	if dataFile, err = os.Create(GetTmpPath(GetDataPath(path))); err != nil {
		return nil, errors.Wrapf(err, "cannot create data file for chunk '%s'", path)
	}
	index, err := CreateIndex(indexFile)

	success = true
	return &Chunk{Index: index, indexFile: indexFile, dataFile: dataFile, Path: path}, nil
}

func OpenChunk(path string) (*Chunk, error) {
	var indexFile, dataFile *os.File
	success := false
	defer func() {
		utils.TryCloseOnFail(success, dataFile)
		utils.TryCloseOnFail(success, indexFile)
	}()

	var err error
	if indexFile, err = OpenIndexFile(GetIndexPath(path)); err != nil {
		return nil, errors.Wrapf(err, "cannot open index file of chunk '%s'", path)
	}
	if dataFile, err = os.Open(GetDataPath(path)); err != nil {
		return nil, errors.Wrapf(err, "cannot open data file of chunk '%s'", path)
	}
	index, err := OpenIndex(indexFile)

	success = true
	return &Chunk{Index: index, indexFile: indexFile, dataFile: dataFile, Path: path}, nil
}

func (c *Chunk) Store(data DataRecord) error {
	offset, err := c.dataFile.Seek(0, os.SEEK_END)
	if err != nil {
		return errors.Wrapf(err, "cannot get write positiion")
	}
	size, err := data.Data.Save(c.dataFile)
	if err != nil {
		return errors.Wrap(err, "cannot store data to chunk")
	}

	record, err := c.Index.AppendRecord()
	if err != nil {
		return errors.Wrapf(err, "cannot append record to index")
	}
	record.Offset = offset
	record.Size = int64(size)
	record.TTL = data.TTL
	record.LastTry = data.LastTry
	return nil
}

func (c *Chunk) Restore(record IndexRecord) ([]byte, error) {
	// TODO: кажется это лучше будет сделать с использованием mmapped-файлов.
	buffer := make([]byte, record.Size)
	_, err := c.dataFile.ReadAt(buffer, record.Offset)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot read data")
	}
	return buffer, nil
}

func (c *Chunk) Flush() {
	c.Index.Flush()
	c.indexFile.Sync()
	c.dataFile.Sync()
}

func (c *Chunk) Close() error {
	deleteChunk := c.Index.Header.ActiveCount == 0
	c.Index.Close()
	c.dataFile.Close()
	c.indexFile.Close()
	if deleteChunk {
		os.Remove(c.dataFile.Name())
		os.Remove(c.indexFile.Name())
	}
	return nil
}

func (c *Chunk) Finalize() error {
	deleteChunk := c.Index.Header.ActiveCount == 0
	if err := c.Close(); err != nil {
		return err
	}
	if deleteChunk {
		return nil
	}

	if err := c.finalizeFile("data", GetDataPath(c.Path)); err != nil {
		return err
	}
	if err := c.finalizeFile("index", GetIndexPath(c.Path)); err != nil {
		return err
	}
	return nil
}

func (c *Chunk) finalizeFile(fileType, path string) error {
	if err := os.Rename(GetTmpPath(path), path); err != nil {
		return errors.Wrapf(err, "cannot move '%s'-file of chunk '%s'", fileType, path)
	}
	return nil
}

type ChunkRecordHandler func(*Chunk, IndexRecord) bool

func (c *Chunk) ForEachActiveRecord(repeatTimeout time.Duration, handler ChunkRecordHandler) {
	// TODO: переделать, так как использование с callback-функцией не очень удобное
	// к тому же наружу можно возвращать уже []byte, который возвращается сейчас методом Restore.
	// А регистрацию обработки можно вынести в отдельный метод.
	now := time.Now()
	timeLimit := now.Add(-repeatTimeout)
	for i, record := range c.Index.Records {
		if record.TTL <= 0 || timeLimit.Before(record.LastTry) {
			continue
		}

		if handler(c, record) {
			c.Index.Records[i].TTL = 0
		} else {
			c.Index.Records[i].TTL -= 1
		}
		c.Index.Records[i].LastTry = now
		if c.Index.Records[i].TTL == 0 {
			c.Index.Header.ActiveCount -= 1
		}
	}
}

func GetIndexPath(path string) string {
	return path + indexSuffix
}

func GetDataPath(path string) string {
	return path + dataSuffix
}

func GetTmpPath(path string) string {
	return path + tmpSuffix
}
