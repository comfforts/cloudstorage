package cloudstorage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/comfforts/logger"
	"github.com/stretchr/testify/require"
)

type testConfig struct {
	dir       string
	bucket    string
	credsPath string
}

func getTestConfig() testConfig {
	dataDir := os.Getenv("DATA_DIR")
	credsPath := os.Getenv("CREDS_PATH")
	bktName := os.Getenv("BUCKET_NAME")

	return testConfig{
		dir:       dataDir,
		bucket:    bktName,
		credsPath: credsPath,
	}
}

type JSONMapper = map[string]interface{}

func TestCloudFileStorage(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		client CloudStorage,
		testCfg testConfig,
	){
		"list objects succeeds":                   testListObjects,
		"file upload & delete succeeds":           testUploadDelete,
		"file upload, download & delete succeeds": testUploadDownloadDelete,
	} {
		testCfg := getTestConfig()
		t.Run(scenario, func(t *testing.T) {
			client, teardown := setupCloudTest(t, testCfg)
			defer teardown()
			fn(t, client, testCfg)
		})
	}
}

func setupCloudTest(t *testing.T, testCfg testConfig) (
	client CloudStorage,
	teardown func(),
) {
	t.Helper()

	err := createDirectory(fmt.Sprintf("%s/", testCfg.dir))
	require.NoError(t, err)

	logger := logger.NewTestAppLogger(testCfg.dir)

	cscCfg := CloudStorageClientConfig{
		CredsPath: testCfg.credsPath,
	}
	csc, err := NewCloudStorageClient(cscCfg, logger)
	require.NoError(t, err)

	return csc, func() {
		err := csc.Close()
		require.NoError(t, err)

		t.Logf(" test ended, will remove %s folder", testCfg.dir)
		err = os.RemoveAll(testCfg.dir)
		require.NoError(t, err)
	}
}

func testUploadDelete(t *testing.T, client CloudStorage, testCfg testConfig) {
	name := "testUpDe"
	filePath, err := createJSONFile(testCfg.dir, name)
	require.NoError(t, err)

	file, err := os.Open(filePath)
	require.NoError(t, err)
	defer func() {
		err := file.Close()
		require.NoError(t, err)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfr, err := NewCloudFileRequest(testCfg.bucket, filepath.Base(filePath), testCfg.dir, 0)
	require.NoError(t, err)

	n, err := client.UploadFile(ctx, file, cfr)
	require.NoError(t, err)
	t.Logf(" testUploadDelete: %d bytes written", n)
	require.Equal(t, true, n > 0)

	err = client.DeleteObject(ctx, cfr)
	require.NoError(t, err)
}

func testUploadDownloadDelete(t *testing.T, client CloudStorage, testCfg testConfig) {
	name := "testUpDoDe"
	dataDir := fmt.Sprintf("%s/%s", testCfg.dir, "delivery")
	filePath, err := createJSONFile(dataDir, name)
	require.NoError(t, err)

	file, err := os.Open(filePath)
	require.NoError(t, err)
	defer func() {
		err := file.Close()
		require.NoError(t, err)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfr, err := NewCloudFileRequest(testCfg.bucket, filepath.Base(filePath), dataDir, 0)
	require.NoError(t, err)

	nUp, err := client.UploadFile(ctx, file, cfr)
	require.NoError(t, err)
	t.Logf(" testUploadDownloadDelete: %d bytes written", nUp)
	require.Equal(t, true, nUp > 0)

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

	nDow, err := client.DownloadFile(ctx, lFile, cfr)
	require.NoError(t, err)
	t.Logf(" testUploadDownloadDelete: %d bytes written to file %s", nDow, localFilePath)
	require.Equal(t, true, nDow > 0)
	require.Equal(t, nUp, nDow)

	err = client.DeleteObject(ctx, cfr)
	require.NoError(t, err)
}

func testListObjects(t *testing.T, client CloudStorage, testCfg testConfig) {
	name := "test"
	filePath, err := createJSONFile(testCfg.dir, name)
	require.NoError(t, err)

	file, err := os.Open(filePath)
	require.NoError(t, err)
	defer func() {
		err := file.Close()
		require.NoError(t, err)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfr, err := NewCloudFileRequest(testCfg.bucket, filepath.Base(filePath), testCfg.dir, 0)
	require.NoError(t, err)

	n, err := client.UploadFile(ctx, file, cfr)
	require.NoError(t, err)
	t.Logf(" testUpload: %d bytes written", n)
	require.Equal(t, true, n > 0)

	names, err := client.ListObjects(ctx, cfr)
	require.NoError(t, err)
	require.Equal(t, true, len(names) > 0)

	err = client.DeleteObject(ctx, cfr)
	require.NoError(t, err)
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

func createJSONFile(dir, name string) (string, error) {
	fPath := fmt.Sprintf("%s.json", name)
	if dir != "" {
		fPath = fmt.Sprintf("%s/%s", dir, fPath)
	}
	items := createStoreJSONList()

	_, err := os.Stat(filepath.Dir(fPath))
	if err != nil {
		if os.IsNotExist(err) {
			err := os.MkdirAll(filepath.Dir(fPath), os.ModePerm)
			if err != nil {
				return "", err
			}
		}
	}

	f, err := os.Create(fPath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	encoder := json.NewEncoder(f)
	err = encoder.Encode(items)
	if err != nil {
		return "", err
	}
	return fPath, nil
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
