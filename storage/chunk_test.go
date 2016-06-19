package storage

import (
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lyobzik/go-utils"
	"github.com/nu7hatch/gouuid"
	"github.com/stretchr/testify/require"
)

// Helpers types for chunk tests.
type chunkTestStringData string

func (d *chunkTestStringData) Close() {
}

func (d *chunkTestStringData) Save(writer io.Writer) (int, error) {
	return writer.Write([]byte(*d))
}

// Helpers for chunk tests.
type ChunkTestFunc func(string) string

func runChunkTest(t *testing.T, testFunc ChunkTestFunc) {
	chunkId, err := uuid.NewV4()
	require.NoError(t, err, "cannot generate unique chunk name")
	storagePath := filepath.Join(testStorage, chunkId.String())

	err = utils.EnsureDir(storagePath)
	require.NoError(t, err, "cannot create test storage")

	chunkPath := testFunc(storagePath)

	checkChunkPartNotExist(t, GetIndexPath(chunkPath))
	checkChunkPartNotExist(t, GetDataPath(chunkPath))

	err = os.RemoveAll(storagePath)
	require.NoError(t, err, "cannot remove test storage '%s'", storagePath)
}

func checkChunkPartNotExist(t *testing.T, path string) {
	exist, err := utils.IsExist(path)
	require.NoError(t, err, "cannot get stat of chunk file")
	require.False(t, exist, "empty chunk must be removed on finalize")
}

func createTestChunk(t *testing.T, storagePath string) *Chunk {
	chunk, err := CreateChunk(storagePath)
	require.NoError(t, err, "cannot create chunk")
	return chunk
}

func openTestChunk(t *testing.T, chunkPath string) *Chunk {
	chunk, err := OpenChunk(chunkPath)
	require.NoError(t, err, "cannot open chunk")
	return chunk
}

func finalizeTestChunk(t *testing.T, chunk *Chunk) {
	err := chunk.Finalize()
	require.NoError(t, err, "cannot finalize chunk")
}

func closeTestChunk(t *testing.T, chunk *Chunk) {
	err := chunk.Close()
	require.NoError(t, err, "cannot close chunk")
}

func storeDataToTestChunk(t *testing.T, chunk *Chunk, data string, ttl int32, lastTry time.Time) {
	value := chunkTestStringData(data)
	err := chunk.Store(DataRecord{Data: &value, TTL: ttl, LastTry: lastTry})
	require.NoError(t, err, "cannot store value to chunk")
}

// Chunk tests.
func TestCreateEmptyChunk(t *testing.T) {
	runChunkTest(t, func(storagePath string) string {
		chunk := createTestChunk(t, storagePath)
		finalizeTestChunk(t, chunk)

		return chunk.Path
	})
}

func TestCreateAndReadEmptyChunk(t *testing.T) {
	runChunkTest(t, func(storagePath string) string {
		chunk := createTestChunk(t, storagePath)
		chunk.Index.Header.ActiveCount = 1 // Weird trick for test.
		closeTestChunk(t, chunk)

		indexPath, dataPath := GetIndexPath(chunk.Path), GetDataPath(chunk.Path)
		err := os.Rename(GetTmpPath(indexPath), indexPath)
		require.NoError(t, err, "cannot rename chunk index file")
		err = os.Rename(GetTmpPath(dataPath), dataPath)
		require.NoError(t, err, "cannot rename chunk data file")

		chunk = openTestChunk(t, chunk.Path)
		chunk.Index.Header.ActiveCount = 0 // Weird trick for test.
		closeTestChunk(t, chunk)

		return chunk.Path
	})
}

func TestChunkStoreAndRestore(t *testing.T) {
	expectedValues := []string{"test", "qwerty", "Есть только две добродетели: деятельность и ум."}
	var ttl int32 = 1
	lastTry := time.Now()

	runChunkTest(t, func(storagePath string) string {
		chunk := createTestChunk(t, storagePath)
		for _, value := range expectedValues {
			storeDataToTestChunk(t, chunk, value, ttl, lastTry)
		}
		chunk.Flush()
		finalizeTestChunk(t, chunk)

		chunk = openTestChunk(t, chunk.Path)
		i := 0
		chunk.ForEachActiveRecord(0, func(chunk *Chunk, record IndexRecord) bool {
			require.Equal(t, record.TTL, ttl, "incorrect TTL value of IndexRecord")
			require.Equal(t, record.LastTry, lastTry, "incorrect LastTry value of IndexRecord")
			data, err := chunk.Restore(record)
			require.NoError(t, err, "cannot restore value from chunk")
			require.Equal(t, expectedValues[i], string(data), "restore incorrect value")
			i += 1
			return true
		})
		closeTestChunk(t, chunk)

		return chunk.Path
	})
}

func TestChunkStoreAndRestoreWithDiffentTTLAndHandleErrors(t *testing.T) {
	expectedValues := []string{"test", "qwerty", "Есть только две добродетели: деятельность и ум."}

	runChunkTest(t, func(storagePath string) string {
		chunk := createTestChunk(t, storagePath)
		for i, value := range expectedValues {
			storeDataToTestChunk(t, chunk, value, int32(i)+1, time.Now())
		}
		chunk.Flush()
		finalizeTestChunk(t, chunk)

		chunk = openTestChunk(t, chunk.Path)
		for i := 0; i < len(expectedValues)+1; i += 1 {
			j := i
			chunk.ForEachActiveRecord(0, func(chunk *Chunk, record IndexRecord) bool {
				data, err := chunk.Restore(record)
				require.NoError(t, err, "cannot restore value from chunk")
				require.Equal(t, string(expectedValues[j]), string(data), "restore incorrect value")
				j += 1
				return record.TTL == 1
			})
			require.Equal(t, j, len(expectedValues), "incorrect count of active records")
		}
		closeTestChunk(t, chunk)

		return chunk.Path
	})
}
