package cloudstorage

import (
	"context"
	"errors"
	"io"

	"cloud.google.com/go/storage"
)

const DEFAULT_DATA_DIR = "data"

type CloudStorage interface {
	// ListObjects lists objects at given cloud bucket
	ListObjects(ctx context.Context, bucketName string) ([]string, error)
	// UploadFile uploads file to given cloud bucket & filepath, creates a new one or replaces existing
	UploadFile(ctx context.Context, file io.Reader, bucketName, path, fileName string) (int64, error)
	// DownloadFile copies content of file at given cloud bucket & filepath to given file
	DownloadFile(ctx context.Context, file io.Writer, bucketName, path, fileName string) (int64, error)
	// DeleteObject delete file at given cloud bucket & filepath
	DeleteObject(ctx context.Context, bucketName, filePath string) error
	// DeleteObjects delete files at given cloud bucket
	DeleteObjects(ctx context.Context, bucketName string) error
	// Reads file data of given length at given offset
	ReadAt(ctx context.Context, cfr CloudFileRequest, p []byte, off int64) (int, error)
	// Close closes storage client connections
	Close(ctx context.Context) error
}

const (
	ERROR_MISSING_BUCKET_NAME string = "bucket name missing"
	ERROR_MISSING_FILE_PATH   string = "file path missing"
	ERROR_MISSING_FILE_NAME   string = "file name missing"
)

var (
	ErrBucketNameMissing = errors.New(ERROR_MISSING_BUCKET_NAME)
	ErrFilePathMissing   = errors.New(ERROR_MISSING_FILE_PATH)
	ErrFileNameMissing   = errors.New(ERROR_MISSING_FILE_NAME)
)

type BufferSize int64

const (
	OneKB               BufferSize = 1024      // 1KB
	ThirtyTwoKB         BufferSize = 32 * 1024 // 32KB
	DEFAULT_BUFFER_SIZE            = OneKB
)

type CloudStorageClientConfig struct {
	CredsPath string `json:"creds_path"`
}

type GCPStorageReadAtAdaptor struct {
	Reader *storage.Reader
}

func (ra *GCPStorageReadAtAdaptor) ReadAt(p []byte, off int64) (n int, err error) {
	// Seek to the desired offset
	_, err = io.CopyN(io.Discard, ra.Reader, off)
	if err != nil {
		return 0, err
	}

	// Read the requested data
	return ra.Reader.Read(p)
}

type CloudFileRequest struct {
	bucket  string
	file    string
	path    string
	modTime int64
}

// NewCloudFileRequest takes bucket name, file name & filepath, return cloud storage request
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
