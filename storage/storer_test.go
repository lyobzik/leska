package storage

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nu7hatch/gouuid"
	"github.com/op/go-logging"
	"github.com/stretchr/testify/require"
)

// Helpers for chunk tests.
type StorerTestFunc func(string)

func runStorerTest(t *testing.T, testFunc StorerTestFunc) {
	chunkId, err := uuid.NewV4()
	require.NoError(t, err, "cannot generate unique chunk name")
	storagePath := filepath.Join(testStorage, chunkId.String())

	testFunc(storagePath)

	err = os.RemoveAll(storagePath)
	require.NoError(t, err, "cannot remove test storage '%s'", storagePath)
}

func createStorerLogger(t *testing.T) *logging.Logger {
	logger, err := logging.GetLogger("")
	require.NoError(t, err, "cannot create logger")

	backend := logging.NewLogBackend(os.Stderr, "", 0)

	format := " %{color}%{time:15:04:05.000} [%{level}]%{color:reset} %{message}"
	formatter := logging.MustStringFormatter(format)
	formattedBackend := logging.NewBackendFormatter(backend, formatter)

	leveledBackend := logging.AddModuleLevel(formattedBackend)
	leveledBackend.SetLevel(logging.DEBUG, "")

	logger.SetBackend(leveledBackend)
	return logger
}

func addValuesToTestStorer(t *testing.T, storer *Storer, values []string) {
	for _, value := range values {
		data := chunkTestStringData(value)
		storer.Add(&data)
	}
}

// Storer tests.
func TestCreateStorer(t *testing.T) {
	runStorerTest(t, func(storagePath string) {
		logger := createStorerLogger(t)
		_, err := NewStorer(logger, storagePath, 1, 0, 0)
		require.NoError(t, err, "cannot create storer")
	})
}

func TestRunAndStopStorer(t *testing.T) {
	runStorerTest(t, func(storagePath string) {
		logger := createStorerLogger(t)
		storer, err := StartStorer(logger, storagePath, 1, 0, 0)
		require.NoError(t, err, "cannot start storer")
		storer.Stop()
	})
}

func TestAddDataToStorer(t *testing.T) {
	expectedValues := []string{"test", "qwerty", "Есть только две добродетели: деятельность и ум."}
	chunkLifetime := 10 * time.Millisecond

	runStorerTest(t, func(storagePath string) {
		logger := createStorerLogger(t)
		storer, err := StartStorer(logger, storagePath, 1, chunkLifetime, 1)
		require.NoError(t, err, "cannot start storer")
		// Append records to one chunk.
		addValuesToTestStorer(t, storer, expectedValues)
		// Wait while first chunk finalized,
		time.Sleep(2 * chunkLifetime)
		// and append records to second chunk.
		addValuesToTestStorer(t, storer, expectedValues)
		// Wait while second chunk finalized,
		time.Sleep(2 * chunkLifetime)

		chunkName := <-storer.Chunks

		chunk := openTestChunk(t, chunkName)
		i := 0
		chunk.ForEachActiveRecord(0, func(chunk *Chunk, record IndexRecord) bool {
			data, err := chunk.Restore(record)
			require.NoError(t, err, "cannot restore value from chunk")
			require.Equal(t, expectedValues[i], string(data), "restore incorrect value")
			i += 1
			return true
		})
		closeTestChunk(t, chunk)

		storer.Stop()
	})
}
