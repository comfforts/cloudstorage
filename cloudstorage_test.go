package cloudstorage_test

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/comfforts/logger"
	"github.com/stretchr/testify/require"

	cs "github.com/comfforts/cloudstorage"
)

const DEFAULT_DATA_PATH = "scheduler"

type testConfig struct {
	dir       string
	bucket    string
	credsPath string
}

type JSONMapper = map[string]interface{}

func TestListObjects(t *testing.T) {
	testCfg := buildTestConfig()
	client, ctx, teardown := setupCloudTest(t, testCfg)
	defer teardown()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	names, err := client.ListObjects(ctx, testCfg.bucket)
	require.NoError(t, err)
	require.Equal(t, true, len(names) > 0)

	if l, err := logger.LoggerFromContext(ctx); err == nil {
		l.Debug("bucket items", "items", names)
	}
}

func TestDownloadObject(t *testing.T) {
	testCfg := buildTestConfig()
	client, ctx, teardown := setupCloudTest(t, testCfg)
	defer teardown()

	l, err := logger.LoggerFromContext(ctx)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	names, err := client.ListObjects(ctx, testCfg.bucket)
	require.NoError(t, err)
	require.Equal(t, true, len(names) > 0)
	l.Debug("bucket items", "num-items", len(names))
	l.Debug("bucket items", "items", names)

	fileName := "Agents-sm.csv"
	filePath := "scheduler"

	localFilePath := filepath.Join(testCfg.dir, filePath, fileName)
	_, err = os.Stat(filepath.Dir(localFilePath))
	if err != nil {
		if os.IsNotExist(err) {
			err := os.MkdirAll(filepath.Dir(localFilePath), os.ModePerm)
			require.NoError(t, err)
		}
	}
	lFile, err := os.Create(localFilePath)
	require.NoError(t, err)
	defer func() {
		err := lFile.Close()
		require.NoError(t, err)
	}()

	n, err := client.DownloadFile(ctx, lFile, testCfg.bucket, filePath, fileName)
	require.NoError(t, err)
	require.Equal(t, true, n > 0)
	l.Debug("downloaded cloud file", "local-file-path", localFilePath, "bytes", n)
}

func TestUploadDownloadDelete(t *testing.T) {
	testCfg := buildTestConfig()
	client, ctx, teardown := setupCloudTest(t, testCfg)
	defer teardown()

	l, err := logger.LoggerFromContext(ctx)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	name := "testUploadDownloadDelete"
	dataDir := fmt.Sprintf("%s/%s", testCfg.dir, "delivery")

	// create json data
	items := createStoreJSONList()
	// Serialize json data
	itemsJSON, err := json.MarshalIndent(items, "", "  ")
	require.NoError(t, err)

	// build json buffer reader
	file := bytes.NewReader(itemsJSON)
	uploadedFilePath := fmt.Sprintf("%s/%s.json", testCfg.dir, name)

	nUp, err := client.UploadFile(ctx, file, testCfg.bucket, testCfg.dir, filepath.Base(uploadedFilePath))
	require.NoError(t, err)
	require.Equal(t, true, nUp > 0)
	l.Debug("uploaded cloud file", "file", uploadedFilePath, "bytes", nUp)

	localFilePath := filepath.Join(dataDir, fmt.Sprintf("%s-copy.json", name))
	_, err = os.Stat(filepath.Dir(localFilePath))
	if err != nil {
		if os.IsNotExist(err) {
			err := os.MkdirAll(filepath.Dir(localFilePath), os.ModePerm)
			require.NoError(t, err)
		}
	}
	lFile, err := os.Create(localFilePath)
	require.NoError(t, err)
	defer func() {
		err := lFile.Close()
		require.NoError(t, err)
	}()

	nDow, err := client.DownloadFile(ctx, lFile, testCfg.bucket, testCfg.dir, filepath.Base(uploadedFilePath))
	require.NoError(t, err)
	require.Equal(t, true, nDow > 0)
	require.Equal(t, nUp, nDow)
	l.Debug("downloaded cloud file", "local-file-path", localFilePath, "bytes", nDow)

	err = client.DeleteObject(ctx, testCfg.bucket, uploadedFilePath)
	require.NoError(t, err)
	l.Debug("deleted cloud file", "file", uploadedFilePath)
}

func TestCloudFileStorage(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		ctx context.Context,
		client cs.CloudStorage,
		testCfg testConfig,
	){
		"upload list delete objects succeeds": testUploadListDeleteObjects,
	} {
		testCfg := buildTestConfig()
		t.Run(scenario, func(t *testing.T) {
			client, ctx, teardown := setupCloudTest(t, testCfg)
			defer teardown()
			fn(t, ctx, client, testCfg)
		})
	}
}

func testUploadListDeleteObjects(t *testing.T, ctx context.Context, client cs.CloudStorage, testCfg testConfig) {
	l, err := logger.LoggerFromContext(ctx)
	require.NoError(t, err)

	name := "test"
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// create json data
	items := createStoreJSONList()
	// Serialize json data
	itemsJSON, err := json.MarshalIndent(items, "", "  ")
	require.NoError(t, err)

	// build json buffer reader
	file := bytes.NewReader(itemsJSON)
	uploadedFilePath := fmt.Sprintf("%s/%s.json", testCfg.dir, name)

	n, err := client.UploadFile(ctx, file, testCfg.bucket, testCfg.dir, filepath.Base(uploadedFilePath))
	require.NoError(t, err)
	l.Debug("uploaded cloud file", "file", uploadedFilePath, "bytes", n)
	require.Equal(t, true, n > 0)

	names, err := client.ListObjects(ctx, testCfg.bucket)
	require.NoError(t, err)
	require.Equal(t, true, len(names) > 0)
	require.True(t, slices.Contains(names, uploadedFilePath))
	l.Debug("bucket items", "items", names)

	err = client.DeleteObject(ctx, testCfg.bucket, uploadedFilePath)
	require.NoError(t, err)
	l.Debug("deleted cloud file", "file", uploadedFilePath)
}

func TestReadFileChunksGCP(t *testing.T) {
	fileName := "Agents-sm.csv"
	filePath := "scheduler"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	l := logger.GetSlogLogger()
	ctx = logger.WithLogger(ctx, l)

	chnkStream, err := readFileChunksGCP(t, ctx, fileName, filePath)
	require.NoError(t, err)

	processCSVStream(ctx, chnkStream)
	if err != nil {
		fmt.Printf("error processing streaming: %v\n", err)
	}
}

func TestReadFileChunkRecordsGCP(t *testing.T) {
	fileName := "Agents-sm.csv"
	filePath := "scheduler"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	l := logger.GetSlogLogger()
	ctx = logger.WithLogger(ctx, l)

	chnkStream, err := readFileChunksGCP(t, ctx, fileName, filePath)
	require.NoError(t, err)

	processCSVStreamRecord(ctx, chnkStream)
	if err != nil {
		fmt.Printf("error processing streaming: %v\n", err)
	}
}

func TestReadFileChunks(t *testing.T) {
	fileName := "Agents-sm.csv"
	filePath := "scheduler"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	chnkStream, err := readFileChunks(fileName, filePath)
	require.NoError(t, err)

	processCSVStream(ctx, chnkStream)
	if err != nil {
		fmt.Printf("error processing streaming: %v\n", err)
	}
}

func TestReadFileChunkRecords(t *testing.T) {
	fileName := "Agents-sm.csv"
	filePath := "scheduler"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	chnkStream, err := readFileChunks(fileName, filePath)
	require.NoError(t, err)

	processCSVStreamRecord(ctx, chnkStream)
	if err != nil {
		fmt.Printf("error processing streaming: %v\n", err)
	}
}

func setupCloudTest(t *testing.T, cfg testConfig) (client cs.CloudStorage, ctx context.Context, teardown func()) {
	t.Helper()

	require.NoError(t, createDirectory(fmt.Sprintf("%s/", cfg.dir)))

	l := logger.GetSlogMultiLogger(cfg.dir)
	ctx = logger.WithLogger(context.Background(), l)

	cscCfg := cs.CloudStorageClientConfig{
		CredsPath: cfg.credsPath,
	}
	csc, err := cs.NewCloudStorageClient(ctx, cscCfg)
	require.NoError(t, err)

	return csc, ctx, func() {
		err := csc.Close(ctx)
		require.NoError(t, err)
	}
}

func buildTestConfig() testConfig {
	dataDir := os.Getenv("DATA_DIR")
	credsPath := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	bktName := os.Getenv("BUCKET")

	return testConfig{
		dir:       dataDir,
		bucket:    bktName,
		credsPath: credsPath,
	}
}

func createDirectory(path string) error {
	_, err := os.Stat(filepath.Dir(path))
	if err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll(filepath.Dir(path), os.ModePerm)
			if err == nil {
				return nil
			}
		}
		return err
	}
	return nil
}

func createStoreJSONList() []JSONMapper {
	items := []JSONMapper{
		{
			"city":      "Hong Kong",
			"org":       "starbucks",
			"name":      "Plaza Hollywood",
			"country":   "CN",
			"longitude": 114.20169067382812,
			"latitude":  22.340700149536133,
			"store_id":  1,
		},
		{
			"city":      "Hong Kong",
			"org":       "starbucks",
			"name":      "Exchange Square",
			"country":   "CN",
			"longitude": 114.15818786621094,
			"latitude":  22.283939361572266,
			"store_id":  6,
		},
		{
			"city":      "Kowloon",
			"org":       "starbucks",
			"name":      "Telford Plaza",
			"country":   "CN",
			"longitude": 114.21343994140625,
			"latitude":  22.3228702545166,
			"store_id":  8,
		},
	}
	return items
}

func readFileChunksGCP(t *testing.T, ctx context.Context, fileName, filePath string) (<-chan []byte, error) {
	const BUFFER_SIZE = 400
	testCfg := buildTestConfig()
	client, _, teardown := setupCloudTest(t, testCfg)
	defer teardown()

	// Create a channel to stream the chunks
	chnkStream := make(chan []byte)

	// Start a goroutine to read and send chunks of data
	go func() {
		defer func() {
			close(chnkStream)
		}()

		cfr, err := cs.NewCloudFileRequest(testCfg.bucket, fileName, filePath, 0)
		require.NoError(t, err)

		buf := make([]byte, BUFFER_SIZE)
		var offset int64

		for {
			n, err := client.ReadAt(ctx, cfr, buf, offset)
			if err != nil {
				if err != io.EOF {
					fmt.Printf("error reading file %s - from offset: %d - err: %v\n", fileName, offset, err)
				} else {
					fmt.Printf("end of  file %s - offset: %d\n", fileName, offset)
					if n > 0 {
						// push data to stream
						chnkStream <- buf[:n]
					}
				}
				break
			}
			if n == 0 {
				fmt.Printf("no more data in file %s - offset: %d\n", fileName, offset)
				break
			}
			// increment offset
			offset += int64(n)
			// push data to stream
			chnkStream <- buf[:n]
			// reset buffer
			buf = make([]byte, BUFFER_SIZE)
		}
	}()

	// Return the channel to stream the chunks
	return chnkStream, nil
}

func readFileChunks(fileName, filePath string) (<-chan []byte, error) {
	const LOCAL_DATA_DIR = "data"
	const BUFFER_SIZE = 400

	localFilePath := filepath.Join(LOCAL_DATA_DIR, filePath, fileName)
	_, err := os.Stat(filepath.Dir(localFilePath))
	if err != nil {
		return nil, fmt.Errorf("error accessing file %s: %w", localFilePath, err)
	}

	// Create a channel to stream the chunks
	chnkStream := make(chan []byte)

	// Start a goroutine to read and send chunks of data
	go func() {
		defer func() {
			close(chnkStream)
		}()
		// open file
		file, err := os.Open(localFilePath)
		if err != nil {
			fmt.Printf("error opening file %s: %v", localFilePath, err)
			return
		}
		defer func() {
			err := file.Close()
			if err != nil {
				fmt.Printf("error closing file %s: %v\n", localFilePath, err)
			}
		}()
		buf := make([]byte, BUFFER_SIZE)
		var offset int64
		for {
			n, err := file.ReadAt(buf, offset)
			if err != nil {
				if err != io.EOF {
					fmt.Printf("error reading file %s - from offset: %d - err: %v\n", localFilePath, offset, err)
				} else {
					fmt.Printf("end of  file %s - offset: %d\n", localFilePath, offset)
					if n > 0 {
						// push data to stream
						chnkStream <- buf[:n]
					}
				}
				break
			}
			if n == 0 {
				fmt.Printf("no more data in file %s - offset: %d\n", localFilePath, offset)
				break
			}
			// increment offset
			offset += int64(n)
			// push data to stream
			chnkStream <- buf[:n]
			// reset buffer
			buf = make([]byte, BUFFER_SIZE)
		}
	}()

	// Return the channel to stream the chunks
	return chnkStream, nil
}

func processCSVStream(ctx context.Context, chunkStream <-chan []byte) {
	chnkCnt := 0
	for {
		select {
		case chnk, ok := <-chunkStream:
			if ok {
				chnkCnt++
				// fmt.Printf("chunk# %d, size: %d, chunk: %s\n", chnkCnt, len(chnk), chnk)
				fmt.Printf("chunk# %d, size: %d, chunk: %v\n", chnkCnt, len(chnk), string(chnk))
			} else {
				fmt.Printf("channel closed\n")
				return
			}
		case <-ctx.Done():
			fmt.Println("timeout")
			return
		}
	}
}

func processCSVStreamRecord(ctx context.Context, chunkStream <-chan []byte) {
	chnkCnt := 0
	incompleteRecord := []byte{}
	for {
		select {
		case chnk, ok := <-chunkStream:
			if ok {
				chnkCnt++
				// fmt.Printf("chunk# %d, size: %d, chunk: %s\n", chnkCnt, len(chnk), chnk)
				fmt.Printf("chunk# %d, size: %d\n", chnkCnt, len(chnk))

				// create csv reader
				buffer := bytes.NewBuffer(chnk)
				csvReader := csv.NewReader(buffer)
				csvReader.Comma = '|'
				csvReader.FieldsPerRecord = -1

				// read csv records
				// initialize buffer offset
				bufOffset := csvReader.InputOffset()
				lastOffset := bufOffset
				recCnt := 0
				for {
					// read csv record
					record, err := csvReader.Read()

					// increment chunk record count
					recCnt++

					// handle record read error
					if err != nil {
						if err == io.EOF {
							fmt.Printf("end of chunk# %d, current buffer offset: %d, lastOffset: %d, record: %s\n", chnkCnt, bufOffset, lastOffset, record)
							if lastOffset < bufOffset {
								fmt.Printf("last record was incomplete - chunk# %d, current buffer offset: %d, lastOffset: %d\n", chnkCnt, bufOffset, lastOffset)
							}
						} else {
							fmt.Printf("error reading chunk: %v - chunk# %d, current buffer offset: %d, lastOffset: %d\n", err, chnkCnt, bufOffset, lastOffset)
						}
						break
					}
					// update current buffer offset
					lastOffset, bufOffset = bufOffset, csvReader.InputOffset()

					// Process CSV record (e.g., print the record)
					if (bufOffset == int64(len(chnk)) && lastOffset < bufOffset) || len(record) < 8 {
						// if record's last offsite is not next line, or
						// records has missing details
						if recCnt == 1 {
							// if first record of a chunk,
							// remaining data for dangling record from last chunk
							// is in first incomplete record of next chunk
							fmt.Printf("incomplete record - chunk# %d, current buffer offset: %d, lastOffset: %d, incomplete chunk length: %d, record length: %d, record count: %d, CSV record: %v\n", chnkCnt, bufOffset, lastOffset, len(incompleteRecord), len(record), recCnt, record)

							// build complete record
							completeRecord := append(incompleteRecord, chnk[:bufOffset]...)
							fmt.Printf("complete record - chunk# %d, record length: %d, record: %s\n", chnkCnt, len(completeRecord), string(completeRecord))

							// reset dangling record buffer
							incompleteRecord = []byte{}

							// process completed/valid dangling record
							recs, err := readCSVRecord(completeRecord)
							if err != nil {
								fmt.Printf("error reading dangling record: %v - chunk# %d, record: %s\n", err, chnkCnt, string(completeRecord))
							} else {
								fmt.Printf("processed dangling record - chunk# %d, current buffer offset: %d, lastOffset: %d, recs len: %d\n", chnkCnt, bufOffset, lastOffset, len(recs))
								// call record handler
								err := processCSVRecord(recs[0])
								if err != nil {
									fmt.Printf("error processing dangling CSV record - %v\n", err)
								}
							}
						} else {
							// populate incomplete record buffer for record processing in next chunk
							incompleteRecord = chnk[lastOffset:]
							fmt.Printf("incomplete record - chunk# %d, current buffer offset: %d, lastOffset: %d, incomplete chunk length: %d, record length: %d, record count: %d, CSV record: %v\n", chnkCnt, bufOffset, lastOffset, len(incompleteRecord), len(record), recCnt, record)
						}
					} else {
						fmt.Printf("chunk# %d, current buffer offset: %d, lastOffset: %d\n", chnkCnt, bufOffset, lastOffset)
						// call record handler
						err := processCSVRecord(record)
						if err != nil {
							fmt.Printf("error processing CSV record - chunk# %d, current buffer offset: %d, lastOffset: %d\n", chnkCnt, bufOffset, lastOffset)
						}
					}
				}

			} else {
				// on channel close, if last incomplete record wasn't processed, process record
				if len(incompleteRecord) > 0 {
					fmt.Printf("processing has remaining data record length: %d, CSV record: %v\n", len(incompleteRecord), string(incompleteRecord))
					// process remaining data
					recs, err := readCSVRecord(incompleteRecord)
					if err != nil {
						fmt.Printf("error remaining data: %v \n", err)
					} else {
						fmt.Printf("processed remaining data, recs len: %d\n", len(recs))
						// call record handler
						err := processCSVRecord(recs[0])
						if err != nil {
							fmt.Printf("error processing remaining CSV record - %v\n", err)
						}
					}
				}
				fmt.Printf("channel closed\n")
				return
			}
		case <-ctx.Done():
			fmt.Println("timeout")
			return
		}
	}
}

func readCSVRecord(data []byte) ([][]string, error) {
	// create csv reader
	buf := bytes.NewBuffer(data)
	csvReader := csv.NewReader(buf)
	csvReader.Comma = '|'
	csvReader.FieldsPerRecord = -1

	records := [][]string{}
	var readErr error
	for {
		// read csv record
		record, err := csvReader.Read()
		// handle record read error
		if err != nil {
			if err != io.EOF {
				readErr = err
			}
			break
		}
		records = append(records, record)
	}
	return records, readErr
}

func processCSVRecord(data []string) error {
	fmt.Printf("processed cvs record data, record length: %d, CSV record: %v\n", len(data), data)
	return nil
}
