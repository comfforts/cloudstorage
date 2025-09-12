package cloudstorage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"

	"github.com/comfforts/logger"
)

type cloudStorageClient struct {
	client *storage.Client
	config CloudStorageClientConfig
}

// NewCloudStorageClient takes client config & logger, returns cloud storage client
func NewCloudStorageClient(
	ctx context.Context,
	cfg CloudStorageClientConfig,
) (*cloudStorageClient, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("NewCloudStorageClient - error getting logger from context: %w", err)
	}

	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", cfg.CredsPath)
	client, err := storage.NewClient(context.Background())
	if err != nil {
		l.Error("NewCloudStorageClient - error creating storage client", "error", err.Error())
		return nil, err
	}

	loaderClient := &cloudStorageClient{
		client: client,
		config: cfg,
	}

	return loaderClient, nil
}

func (cs *cloudStorageClient) ListObjects(ctx context.Context, bucketName string) ([]string, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("ListObjects - error getting logger from context: %w", err)
	}

	if bucketName == "" {
		return nil, ErrBucketNameMissing
	}

	bucket := cs.client.Bucket(bucketName)
	it := bucket.Objects(ctx, nil)
	names := []string{}
	for {
		objAttrs, err := it.Next()
		if err != nil {
			if err == iterator.Done {
				break
			} else {
				l.Error("ListObjects - error listing objects", "error", err.Error())
				return names, err
			}
		}
		names = append(names, objAttrs.Name)
	}
	return names, nil
}

func (cs *cloudStorageClient) UploadFile(
	ctx context.Context,
	file io.Reader,
	bucketName, path, fileName string,
) (int64, error) {
	// check for context logger, bucket name & file name
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return 0, fmt.Errorf("UploadFile - error getting logger from context: %w", err)
	}
	if bucketName == "" {
		return 0, ErrBucketNameMissing
	}
	if fileName == "" {
		return 0, ErrFileNameMissing
	}

	// construct full file path
	fPath := fileName
	if path != "" {
		fPath = filepath.Join(path, fileName)
	}

	// create context with timeout
	ctx, cancel := context.WithTimeout(ctx, time.Second*50)
	defer cancel()

	// check for cloud object existence
	obj := cs.client.Bucket(bucketName).Object(fPath)
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		l.Debug("UploadFile - cloud file doesn't exist, will create new", "filepath", fPath)
	} else {
		l.Debug(
			"UploadFile - cloud file exists, will create new version",
			"created-at", attrs.Created.Unix(),
			"updated-at", attrs.Updated.Unix(),
			"filepath", fPath,
		)
	}

	// create cloud object writer
	wc := obj.NewWriter(ctx)
	defer func() {
		if err := wc.Close(); err != nil {
			l.Error("UploadFile - error closing cloud file", "error", err.Error(), "filepath", fPath)
		}
	}()

	// copy file content to cloud object
	nBytes, err := io.Copy(wc, file)
	if err != nil {
		l.Error("UploadFile - error uploading file", "error", err.Error(), "filepath", fPath)
		return 0, err
	}
	l.Debug("UploadFile - cloud file created/updated", "filepath", fPath)

	// return number of bytes written
	return nBytes, nil
}

func (cs *cloudStorageClient) DownloadFile(
	ctx context.Context,
	file io.Writer,
	bucketName, path, fileName string,
) (int64, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return 0, fmt.Errorf("DownloadFile - error getting logger from context: %w", err)
	}
	if bucketName == "" {
		return 0, ErrBucketNameMissing
	}
	if fileName == "" {
		return 0, ErrFileNameMissing
	}

	fPath := fileName
	if path != "" {
		fPath = filepath.Join(path, fileName)
	}

	ctx, cancel := context.WithTimeout(ctx, time.Second*50)
	defer cancel()

	// download an object with storage.Reader.
	obj := cs.client.Bucket(bucketName).Object(fPath)
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		l.Error("DownloadFile - cloud file inaccessible", "error", err.Error(), "filepath", fPath)
		return 0, err
	}
	l.Debug(
		"DownloadFile - downloading cloud file",
		"filepath", fPath,
		"created-at", attrs.Created.Unix(),
		"updated-at", attrs.Updated.Unix(),
	)

	rc, err := obj.NewReader(ctx)
	if err != nil {
		l.Error("DownloadFile - error reading cloud file", "error", err.Error(), "filepath", fPath)
		return 0, err
	}
	defer func() {
		if err := rc.Close(); err != nil {
			l.Error("DownloadFile - error closing cloud file", "error", err.Error(), "filepath", fPath)
		}
	}()

	nBytes, err := io.Copy(file, rc)
	if err != nil {
		l.Error("DownloadFile - error copying cloud file", "error", err.Error(), "filepath", fPath)
		return 0, err
	}

	return nBytes, nil
}

func (cs *cloudStorageClient) DeleteObject(ctx context.Context, bucketName, filePath string) error {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return fmt.Errorf("DeleteObject - error getting logger from context: %w", err)
	}

	if bucketName == "" {
		return ErrBucketNameMissing
	}
	if filePath == "" {
		return ErrFilePathMissing
	}

	bucket := cs.client.Bucket(bucketName)
	if err := bucket.Object(filePath).Delete(ctx); err != nil {
		l.Error("DeleteObject - error deleting object", "error", err.Error())
		return err
	}
	return nil
}

func (cs *cloudStorageClient) DeleteObjects(ctx context.Context, bucketName string) error {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return fmt.Errorf("DeleteObjects - error getting logger from context: %w", err)
	}

	if bucketName == "" {
		return ErrBucketNameMissing
	}
	bucket := cs.client.Bucket(bucketName)
	it := bucket.Objects(ctx, nil)
	for {
		objAttrs, err := it.Next()
		if err != nil {
			if err == iterator.Done {
				break
			} else {
				l.Error("DeleteObjects - error listing objects", "error", err.Error())
				return err
			}
		}
		l.Info("DeleteObjects - object attributes", "objAttrs", objAttrs)
		if err := bucket.Object(objAttrs.Name).Delete(ctx); err != nil {
			l.Error("DeleteObjects - error deleting object", "error", err.Error())
			return err
		}
	}
	return nil
}

func (cs *cloudStorageClient) ReadAt(
	ctx context.Context,
	cfr CloudFileRequest,
	p []byte,
	off int64,
) (int, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return 0, fmt.Errorf("ReadAt - error getting logger from context: %w", err)
	}
	if cfr.file == "" {
		return 0, ErrFileNameMissing
	}
	if cfr.bucket == "" {
		return 0, ErrBucketNameMissing
	}

	fPath := cfr.file
	if cfr.path != "" {
		fPath = filepath.Join(cfr.path, cfr.file)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// check for object existence
	obj := cs.client.Bucket(cfr.bucket).Object(fPath)
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		l.Error("ReadAt - cloud file inaccessible", "error", err.Error(), "filepath", fPath)
		return 0, err
	}
	l.Debug(
		"ReadAt - reading cloud file chunk",
		"filepath", fPath,
		"created", attrs.Created.Unix(),
		"updated", attrs.Updated.Unix(),
	)

	// open a reader for the object in the bucket
	rc, err := obj.NewReader(ctx)
	if err != nil {
		l.Error("ReadAt - error reading cloud file", "error", err.Error(), "filepath", fPath)
		return 0, err
	}
	rcReadAt := &GCPStorageReadAtAdaptor{rc}
	defer func() {
		if err := rcReadAt.Reader.Close(); err != nil {
			l.Error(
				"ReadAt - error closing cloud file reader",
				"error", err.Error(),
				"filepath", fPath,
			)
		}
	}()

	return rcReadAt.ReadAt(p, off)
}

func (cs *cloudStorageClient) Close(ctx context.Context) error {
	return cs.client.Close()
}
