package cloudstorage

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"time"

	"cloud.google.com/go/storage"
	"github.com/comfforts/errors"
	"github.com/comfforts/logger"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"
)

type CloudStorage interface {
	UploadFile(ct context.Context, file io.Reader, cfr CloudFileRequest) (int64, error)
	DownloadFile(ct context.Context, file io.Writer, cfr CloudFileRequest) (int64, error)
	ListObjects(ctx context.Context, cfr CloudFileRequest) ([]string, error)
	DeleteObjects(ctx context.Context, cfr CloudFileRequest) error
	Close() error
}

type CloudStorageClientConfig struct {
	CredsPath string `json:"creds_path"`
}

type cloudStorageClient struct {
	client *storage.Client
	config CloudStorageClientConfig
	logger logger.AppLogger
}

func NewCloudStorageClient(cfg CloudStorageClientConfig, logger logger.AppLogger) (*cloudStorageClient, error) {
	if logger == nil {
		return nil, errors.NewAppError(errors.ERROR_MISSING_REQUIRED)
	}
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", cfg.CredsPath)
	client, err := storage.NewClient(context.Background())
	if err != nil {
		logger.Error(ERROR_CREATING_STORAGE_CLIENT, zap.Error(err))
		return nil, errors.WrapError(err, ERROR_CREATING_STORAGE_CLIENT)
	}

	loaderClient := &cloudStorageClient{
		client: client,
		config: cfg,
		logger: logger,
	}

	return loaderClient, nil
}

func (cs *cloudStorageClient) UploadFile(ct context.Context, file io.Reader, cloudFileRequest CloudFileRequest) (int64, error) {
	if cloudFileRequest.file == "" {
		return 0, ErrFileNameMissing
	}
	fPath := cloudFileRequest.file
	if cloudFileRequest.path != "" {
		fPath = filepath.Join(cloudFileRequest.path, cloudFileRequest.file)
	}

	ctx, cancel := context.WithTimeout(ct, time.Second*50)
	defer cancel()

	// Upload an object with storage.Writer.
	obj := cs.client.Bucket(cloudFileRequest.bucket).Object(fPath)
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		cs.logger.Error(ERROR_FILE_INACCESSIBLE, zap.Error(err), zap.String("filepath", fPath))
	} else {
		objcr := attrs.Created.Unix()
		objmod := attrs.Updated.Unix()
		cs.logger.Info("object created time", zap.Any("created", objcr), zap.Any("updated", objmod), zap.String("filepath", fPath))
	}

	wc := obj.NewWriter(ctx)
	defer func() {
		if err := wc.Close(); err != nil {
			cs.logger.Error(ERROR_CLOSING_FILE, zap.Error(err), zap.String("filepath", fPath))
		}
	}()

	nBytes, err := io.Copy(wc, file)
	if err != nil {
		cs.logger.Error(ERROR_UPLOADING_FILE, zap.Error(err), zap.String("filepath", fPath))
		return 0, errors.WrapError(err, ERROR_UPLOADING_FILE, fPath)
	}

	return nBytes, nil
}

func (cs *cloudStorageClient) DownloadFile(ct context.Context, file io.Writer, cloudFileRequest CloudFileRequest) (int64, error) {
	if cloudFileRequest.file == "" {
		return 0, ErrFileNameMissing
	}
	fPath := cloudFileRequest.file
	if cloudFileRequest.path != "" {
		fPath = filepath.Join(cloudFileRequest.path, cloudFileRequest.file)
	}

	ctx, cancel := context.WithTimeout(ct, time.Second*50)
	defer cancel()

	// download an object with storage.Reader.
	obj := cs.client.Bucket(cloudFileRequest.bucket).Object(fPath)
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		cs.logger.Error(ERROR_FILE_INACCESSIBLE, zap.Error(err), zap.String("filepath", fPath))
		return 0, errors.WrapError(err, ERROR_FILE_INACCESSIBLE, fPath)
	}
	objcr := attrs.Created.Unix()
	objmod := attrs.Updated.Unix()
	cs.logger.Info("object created time", zap.Any("created", objcr), zap.Any("updated", objmod), zap.String("filepath", fPath))

	rc, err := obj.NewReader(ctx)
	if err != nil {
		cs.logger.Error(ERROR_DOWNLOADING_FILE, zap.Error(err), zap.String("filepath", fPath))
		return 0, errors.WrapError(err, ERROR_DOWNLOADING_FILE, fPath)
	}
	defer func() {
		if err := rc.Close(); err != nil {
			cs.logger.Error(ERROR_CLOSING_FILE, zap.Error(err), zap.String("filepath", fPath))
		}
	}()

	nBytes, err := io.Copy(file, rc)
	if err != nil {
		cs.logger.Error(ERROR_DOWNLOADING_FILE, zap.Error(err), zap.String("filepath", fPath))
		return 0, errors.WrapError(err, ERROR_DOWNLOADING_FILE, fPath)
	}

	return nBytes, nil
}

func (cs *cloudStorageClient) ListObjects(ctx context.Context, cloudFileRequest CloudFileRequest) ([]string, error) {
	bucket := cs.client.Bucket(cloudFileRequest.bucket)
	it := bucket.Objects(ctx, nil)
	names := []string{}
	for {
		objAttrs, err := it.Next()
		if err != nil {
			if err == iterator.Done {
				break
			} else {
				cs.logger.Error(ERROR_LISTING_OBJECTS, zap.Error(err))
				return names, errors.WrapError(err, ERROR_LISTING_OBJECTS)
			}
		}
		names = append(names, objAttrs.Name)
	}
	return names, nil
}

func (cs *cloudStorageClient) DeleteObjects(ctx context.Context, cloudFileRequest CloudFileRequest) error {
	bucket := cs.client.Bucket(cloudFileRequest.bucket)
	it := bucket.Objects(ctx, nil)
	for {
		objAttrs, err := it.Next()
		if err != nil {
			if err == iterator.Done {
				break
			} else {
				cs.logger.Error(ERROR_LISTING_OBJECTS, zap.Error(err))
				return errors.WrapError(err, ERROR_LISTING_OBJECTS)
			}
		}
		cs.logger.Info("object attributes", zap.Any("objAttrs", objAttrs))
		if err := bucket.Object(objAttrs.Name).Delete(ctx); err != nil {
			cs.logger.Error(ERROR_DELETING_OBJECTS, zap.Error(err))
			return errors.WrapError(err, ERROR_DELETING_OBJECTS)
		}
	}
	return nil
}

func (cs *cloudStorageClient) Close() error {
	err := cs.client.Close()
	if err != nil {
		cs.logger.Error("error closing storage client", zap.Error(err))
		return errors.WrapError(err, "error closing storage client")
	}
	return nil
}

type CloudFileRequest struct {
	bucket  string
	file    string
	path    string
	modTime int64
}

func NewCloudFileRequest(bucketName, fileName, path string, modTime int64) (CloudFileRequest, error) {
	if bucketName == "" {
		return CloudFileRequest{}, ErrBucketNameMissing
	}
	return CloudFileRequest{
		bucket:  bucketName,
		file:    fileName,
		path:    path,
		modTime: modTime,
	}, nil
}
