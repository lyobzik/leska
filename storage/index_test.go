package storage

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lyobzik/go-utils"
	"github.com/nu7hatch/gouuid"
	"github.com/stretchr/testify/require"
)

const (
	testStorage = "/tmp/tests/storage"
)

// Helpers for index tests.
type TestFunc func(string)

func runIndexTest(t *testing.T, testFunc TestFunc) {
	err := utils.EnsureDir(testStorage)
	require.NoError(t, err, "cannot create test storage")

	indexId, err := uuid.NewV4()
	require.NoError(t, err, "cannot generate unique index name")
	indexPath := filepath.Join(testStorage, indexId.String())

	testFunc(indexPath)

	err = os.Remove(indexPath)
	require.NoError(t, err, "cannot remove test index '%s'", indexPath)
}

func createIndexFileAndIndex(t *testing.T, indexPath string) (*os.File, *Index) {
	indexFile, err := os.Create(indexPath)
	require.NoError(t, err, "cannot create index file")
	index, err := CreateIndex(indexFile)
	require.NoError(t, err, "cannot create index")
	return indexFile, index
}

func openIndexFileAndIndex(t *testing.T, indexPath string) (*os.File, *Index) {
	indexFile, err := OpenIndexFile(indexPath)
	require.NoError(t, err, "cannot open index file")
	index, err := OpenIndex(indexFile)
	require.NoError(t, err, "cannot open index")
	return indexFile, index
}

func closeIndexAndIndexFile(t *testing.T, index *Index, indexFile *os.File) {
	if index != nil {
		err := index.Close()
		require.NoError(t, err, "cannot close index")
	}
	if indexFile != nil {
		err := indexFile.Close()
		require.NoError(t, err, "cannot close index file")
	}
}

func writeBufferToIndexFile(t *testing.T, data []byte, indexFile *os.File) {
	n, err := indexFile.Write(data)
	require.NoError(t, err, "cannot write data to index file")
	require.Equal(t, len(data), n, "cannot write all data to index filer")
}

func writeInt32ToIndexFile(t *testing.T, data int32, indexFile *os.File) {
	dataBuffer := make([]byte, binary.Size(data))
	binary.LittleEndian.PutUint32(dataBuffer, uint32(data))
	writeBufferToIndexFile(t, []byte(dataBuffer), indexFile)
}

func writeInt64ToIndexFile(t *testing.T, data int64, indexFile *os.File) {
	dataBuffer := make([]byte, binary.Size(data))
	binary.LittleEndian.PutUint64(dataBuffer, uint64(data))
	writeBufferToIndexFile(t, []byte(dataBuffer), indexFile)
}

// Index tests.
func TestCreateEmptyIndex(t *testing.T) {
	runIndexTest(t, func(indexPath string) {
		indexFile, index := createIndexFileAndIndex(t, indexPath)
		closeIndexAndIndexFile(t, index, indexFile)
	})
}

func TestCreateAndReadEmptyIndex(t *testing.T) {
	runIndexTest(t, func(indexPath string) {
		indexFile, index := createIndexFileAndIndex(t, indexPath)
		closeIndexAndIndexFile(t, index, indexFile)

		indexFile, index = openIndexFileAndIndex(t, indexPath)
		require.Zero(t, index.Header.Length, "Length of empty index must be equal 0")
		require.Zero(t, index.Header.ActiveCount, "ActiveCount of empty index must be equal 0")
		require.Empty(t, index.Records, "Records of empty index must be empty")
		closeIndexAndIndexFile(t, index, indexFile)
	})
}

func TestReadIndexWithIncorrectMagic(t *testing.T) {
	runIndexTest(t, func(indexPath string) {
		indexFile, err := os.Create(indexPath)
		require.NoError(t, err, "cannot create index file")
		writeInt32ToIndexFile(t, 0, indexFile)
		writeInt32ToIndexFile(t, 0, indexFile)
		writeInt64ToIndexFile(t, 0, indexFile)
		writeInt64ToIndexFile(t, 0, indexFile)
		closeIndexAndIndexFile(t, nil, indexFile)

		indexFile, err = OpenIndexFile(indexPath)
		require.NoError(t, err, "cannot open index file")
		index, err := OpenIndex(indexFile)
		require.Error(t, err, "index with incorrect magic must be opened with error")
		closeIndexAndIndexFile(t, index, indexFile)
	})
}

func TestReadIndexWithIncorrectVersion(t *testing.T) {
	runIndexTest(t, func(indexPath string) {
		indexFile, err := os.Create(indexPath)
		require.NoError(t, err, "cannot create index file")
		writeInt32ToIndexFile(t, indexMagic, indexFile)
		writeInt32ToIndexFile(t, 0, indexFile)
		writeInt64ToIndexFile(t, 0, indexFile)
		writeInt64ToIndexFile(t, 0, indexFile)
		closeIndexAndIndexFile(t, nil, indexFile)

		indexFile, err = OpenIndexFile(indexPath)
		require.NoError(t, err, "cannot open index file")
		index, err := OpenIndex(indexFile)
		require.Error(t, err, "index with incorrect versnio must be opened with error")
		closeIndexAndIndexFile(t, index, indexFile)
	})
}

func TestCreateIndexAndAppendRecrods(t *testing.T) {
	expectedRecords := []IndexRecord{
		{TTL: 1, LastTry: time.Now(), Offset: 0, Size: 1},
		{TTL: 2, LastTry: time.Now(), Offset: 1, Size: 2},
		{TTL: 3, LastTry: time.Now(), Offset: 3, Size: 3},
	}

	runIndexTest(t, func(indexPath string) {
		// Create index and append records.
		indexFile, index := createIndexFileAndIndex(t, indexPath)
		for _, expectedRecord := range expectedRecords {
			record, err := index.AppendRecord()
			require.NoError(t, err, "cannot add record to index")
			*record = expectedRecord
			err = index.Flush()
			require.NoError(t, err, "cannot sync index data to disk")
		}
		closeIndexAndIndexFile(t, index, indexFile)

		// Open and read index.
		indexFile, index = openIndexFileAndIndex(t, indexPath)
		require.Equal(t, len(expectedRecords), int(index.Header.Length), "Incorrect index Length")
		require.Equal(t, len(expectedRecords), int(index.Header.ActiveCount),
			"Incorrect index ActiveCount")
		require.Equal(t, len(expectedRecords), len(index.Records), "Incorrect index record count")
		for i := range expectedRecords {
			require.EqualValues(t, expectedRecords[i], index.Records[i],
				"Incorrect index record [%d]", i)
		}
		closeIndexAndIndexFile(t, index, indexFile)

	})
}
