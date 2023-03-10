package cloudstorage

import "github.com/comfforts/errors"

const (
	ERROR_FILE_INACCESSIBLE       string = "%s inaccessible"
	ERROR_CLOSING_FILE            string = "closing file %s"
	ERROR_UPLOADING_FILE          string = "uploading file %s"
	ERROR_DOWNLOADING_FILE        string = "downloading file %s"
	ERROR_CREATING_STORAGE_CLIENT string = "error creating storage client"
	ERROR_LISTING_OBJECTS         string = "error listing storage bucket objects"
	ERROR_DELETING_OBJECTS        string = "error deleting storage bucket objects"
	ERROR_MISSING_BUCKET_NAME     string = "bucket name missing"
	ERROR_MISSING_FILE_NAME       string = "file name missing"
	ERROR_CREATING_DATA_DIR       string = "creating data directory %s"
	ERROR_STALE_UPLOAD            string = "storage bucket object has updates"
	ERROR_STALE_DOWNLOAD          string = "file object has updates"
)

var (
	ErrBucketNameMissing = errors.NewAppError(ERROR_MISSING_BUCKET_NAME)
	ErrFileNameMissing   = errors.NewAppError(ERROR_MISSING_FILE_NAME)
)
