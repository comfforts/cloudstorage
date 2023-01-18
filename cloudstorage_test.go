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

const TEST_DIR = "test-data"

type testConfig struct {
	dir       string
	bucket    string
	credsPath string
}

func getTestConfig() testConfig {
	return testConfig{
		dir:       fmt.Sprintf("%s/", TEST_DIR),
		bucket:    "mustum-fustum",            // add a valid bucket name
		credsPath: "creds/mustum-fustum.json", // add valid creds and path
	}
}

type JSONMapper = map[string]interface{}

func TestCloudFileStorage(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		client CloudStorage,
		testCfg testConfig,
	){
		"cloud storage file upload succeeds":   testUpload,
		"cloud storage file download succeeds": testDownload,
		"list objects succeeds":                testListObjects,
		"delete cloud bucket objects succeeds": testDeleteObjects,
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

	err := createDirectory(testCfg.dir)
	require.NoError(t, err)

	appLogger := logger.NewTestAppLogger(TEST_DIR)

	cscCfg := CloudStorageClientConfig{
		CredsPath: testCfg.credsPath,
	}
	fsc, err := NewCloudStorageClient(cscCfg, appLogger)
	require.NoError(t, err)

	return fsc, func() {
		t.Logf(" test ended, will remove %s folder", testCfg.dir)
		err := os.RemoveAll(TEST_DIR)
		require.NoError(t, err)
	}
}

func testUpload(t *testing.T, client CloudStorage, testCfg testConfig) {
	name := "test"
	filePath, err := createJSONFile(testCfg.dir, name)
	require.NoError(t, err)

	file, err := os.Open(filePath)
	require.NoError(t, err)
	defer func() {
		err := file.Close()
		require.NoError(t, err)
	}()

	destName := fmt.Sprintf("%s.json", name)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfr, err := NewCloudFileRequest(testCfg.bucket, destName, testCfg.dir, 0)
	require.NoError(t, err)

	n, err := client.UploadFile(ctx, file, cfr)
	require.NoError(t, err)
	t.Logf(" testUpload: %d bytes written", n)
	require.Equal(t, true, n > 0)
}

func testDownload(t *testing.T, client CloudStorage, testCfg testConfig) {
	name := "test"
	filePath, err := createJSONFile(testCfg.dir, name)
	require.NoError(t, err)

	file, err := os.Open(filePath)
	require.NoError(t, err)
	defer func() {
		err := file.Close()
		require.NoError(t, err)
	}()

	destName := fmt.Sprintf("%s.json", name)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfr, err := NewCloudFileRequest(testCfg.bucket, destName, testCfg.dir, 0)
	require.NoError(t, err)

	n, err := client.UploadFile(ctx, file, cfr)
	require.NoError(t, err)
	t.Logf(" testUpload: %d bytes written", n)
	require.Equal(t, true, n > 0)

	localFilePath := filepath.Join(testCfg.dir, fmt.Sprintf("%s-copy.json", name))
	lFile, err := os.Create(localFilePath)
	require.NoError(t, err)
	defer func() {
		err := lFile.Close()
		require.NoError(t, err)
	}()

	n, err = client.DownloadFile(ctx, lFile, cfr)
	require.NoError(t, err)
	t.Logf(" testDownload: %d bytes written to file %s", n, localFilePath)
	require.Equal(t, true, n > 0)
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

	destName := fmt.Sprintf("%s.json", name)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfr, err := NewCloudFileRequest(testCfg.bucket, destName, testCfg.dir, 0)
	require.NoError(t, err)

	n, err := client.UploadFile(ctx, file, cfr)
	require.NoError(t, err)
	t.Logf(" testUpload: %d bytes written", n)
	require.Equal(t, true, n > 0)

	names, err := client.ListObjects(ctx, cfr)
	require.NoError(t, err)
	require.Equal(t, true, len(names) > 0)
}

func testDeleteObjects(t *testing.T, client CloudStorage, testCfg testConfig) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfr, err := NewCloudFileRequest(testCfg.bucket, "", "", 0)
	require.NoError(t, err)

	err = client.DeleteObjects(ctx, cfr)
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
