package main

import (
	"net/http"
	"os"
	"time"

	"fmt"
	"github.com/edsrzf/mmap-go"
	"github.com/facebookgo/httpdown"
	"github.com/jessevdk/go-flags"
	"github.com/lyobzik/go-utils"
	"github.com/lyobzik/leska/storage"
	"github.com/op/go-logging"
	"reflect"
	"unsafe"
	"io"
)

type Config struct {
	Upstreams     []string      `short:"u" long:"upstream" required:"true" description:"group of servers of final destination"`
	Address       string        `short:"a" long:"address" required:"true" description:"listen address of this server"`
	Storage       string        `short:"s" long:"storage" default:"storage" description:"path to directory to store failed requests"`
	RepeatTimeout time.Duration `short:"t" long:"repeat-timeout" default:"0s" description:"timeout between repeated tries"`
	RepeatNumber  int32         `short:"n" long:"repeat-number" default:"1" description:"maximum number of tries"`
	Verbose       []bool        `short:"v" long:"verbose" description:"write detailed log"`
	LogLevel      logging.Level `hidden:"true"`
}

func ParseArgs() Config {
	config := Config{}
	parser := flags.NewParser(&config, flags.Default)
	_, err := parser.Parse()
	if err != nil {
		if !isFlagsHelpError(err) {
			parser.WriteHelp(os.Stderr)
		}
		os.Exit(1)
	}
	config.LogLevel = convertVerboseToLovLevel(config.Verbose)
	return config
}

func isFlagsHelpError(err error) bool {
	flagsError, converted := err.(*flags.Error)
	return converted && flagsError.Type == flags.ErrHelp
}

func convertVerboseToLovLevel(verbose []bool) logging.Level {
	for _, value := range verbose {
		if !value {
			return logging.ERROR
		}
	}
	logLevel := logging.ERROR + logging.Level(len(verbose))
	if logging.DEBUG < logLevel {
		return logging.DEBUG
	}
	return logLevel
}

type Header struct {
	Magic   uint32
	Version uint32
	Size    uint32
}

type CRecord struct {
	TTL     int32
	LastTry time.Time
	Offset  int64
	Size    int64
}

type TestStringData string

func (d *TestStringData) Close() {
}

func (d *TestStringData) Save(writer io.Writer) (int, error) {
	return writer.Write([]byte(*d))
}

/////////////////////////////////////////////////
// TODO: возможно в эту задачу хорошо подойдет fasthttp. Нужно будет посмотреть
// TODO: на сколько все станет сложнее.
func main() {
	// TODO: написать тесты для storage.
	//go RunTestServer(":8088")
	//go RunTestServer(":8089")
	//go RunTestServer(":8090")

	//testData := TestStringData("test")
	//qwertyData := TestStringData("qwerty")
	//tolstoiData := TestStringData("Все любить — любить Бога во всех проявлениях. Любить человека дорогого можно человеческой любовью; но только врага можно любить любовью Божеской.")
	//
	//utils.EnsureDir("tmp/storage/test")
	//chunk, err := storage.CreateChunk("tmp/storage/test")
	//utils.HandleErrorWithoutLogger("cannot create chunk", err)
	//err = chunk.Store(storage.DataRecord{Data: &testData, TTL: 1, LastTry: time.Now()})
	//utils.HandleErrorWithoutLogger("cannot store data to chunk", err)
	//err = chunk.Store(storage.DataRecord{Data: &qwertyData, TTL: 45, LastTry: time.Now()})
	//utils.HandleErrorWithoutLogger("cannot store data to chunk", err)
	//err = chunk.Store(storage.DataRecord{Data: &tolstoiData, TTL: 3948, LastTry: time.Now()})
	//utils.HandleErrorWithoutLogger("cannot store data to chunk", err)
	//chunk.Finalize()

	chunk, err := storage.OpenChunk("tmp/storage/test/1466263516")
	utils.HandleErrorWithoutLogger("cannot open chunk", err)
	fmt.Printf("%+v\n", chunk.Index)
	fmt.Printf("%+v\n", chunk.Index.Header)
	chunk.ForEachActiveRecord(10 * time.Second, func(chunk *storage.Chunk, record storage.IndexRecord) bool {
		data, err := chunk.Restore(record)
		if err != nil {
			fmt.Printf("cannot restore data for record %+v: %v", record, err)
			return false
		}
		fmt.Printf("Try handle '%s'\n", string(data))
		return len(data) > 5
	})
	chunk.Close()


	return

	chunkFile, err := os.OpenFile("/tmp/chunk", os.O_RDWR|os.O_CREATE, 0644)
	utils.HandleErrorWithoutLogger("cannot open chunk", err)
	defer chunkFile.Close()
	chunkFile.Truncate(1024)

	chunkData, err := mmap.Map(chunkFile, mmap.RDWR, 0)
	utils.HandleErrorWithoutLogger("cannot map file", err)
	chunkHeader := (*Header)(unsafe.Pointer(&chunkData[0]))
	fmt.Printf("read chunk data: %x - %v\n", chunkHeader.Magic, chunkHeader)
	chunkHeader.Magic = 0xdeadbeef
	chunkHeader.Version = 1
	chunkHeader.Size = (uint32)(unsafe.Sizeof(Header{}))

	records := make([]CRecord, 0)
	recordsSlice := (*reflect.SliceHeader)(unsafe.Pointer(&records))
	recordsSlice.Data = uintptr(unsafe.Pointer(&chunkData[0])) + unsafe.Sizeof(Header{})
	recordsSlice.Len = 1

	fmt.Printf("read records: %v\n", records)
	//records[0].TTL = 1
	//records[0].LastTry = time.Now()
	//records[0].Offset = 0
	//records[0].Size = 1024

	err = chunkData.Flush()
	utils.HandleErrorWithoutLogger("cannot flush mmapped", err)

	err = chunkData.Unmap()
	utils.HandleErrorWithoutLogger("cannot unmap", err)

	return

	config := ParseArgs()

	logger, err := CreateLogger(config.LogLevel, "leska")
	utils.HandleErrorWithoutLogger("cannot create logger", err)
	logger.Debugf("start leska with config: %v", config)

	// TODO: прокинуть Repeate-настроки куда нужно
	forwarder, err := CreateForwarder(logger, config.Upstreams)
	utils.HandleError(logger, "cannot create forwarder", err)

	storer, err := storage.StartStorer(logger, config.Storage, config.RepeatNumber)
	utils.HandleError(logger, "cannot create storer", err)
	defer storer.Stop()

	repeater, err := StartRepeater(logger, forwarder, storer,
		config.RepeatTimeout, config.RepeatNumber)
	utils.HandleError(logger, "cannot create repeater", err)
	defer repeater.Stop()

	err = httpdown.ListenAndServe(
		&http.Server{
			Addr:    config.Address,
			Handler: NewStreamer(logger, storer, forwarder),
		},
		&httpdown.HTTP{
			StopTimeout: 10 * time.Second,
			KillTimeout: 1 * time.Second,
		})
	utils.HandleError(logger, "cannot start server", err)
}
