package cloudstorage

import "github.com/comfforts/errors"

const (
	ERROR_CREATING_STORAGE_CLIENT string = "error creating storage client"
	ERROR_LISTING_OBJECTS         string = "error listing storage bucket objects"
	ERROR_DELETING_OBJECT         string = "error deleting storage bucket object"
	ERROR_DELETING_OBJECTS        string = "error deleting storage bucket objects"
	ERROR_MISSING_BUCKET_NAME     string = "bucket name missing"
	ERROR_MISSING_FILE_PATH       string = "file path missing"
	ERROR_MISSING_FILE_NAME       string = "file name missing"
	ERROR_STALE_UPLOAD            string = "storage bucket object has updates"
	ERROR_STALE_DOWNLOAD          string = "file object has updates"
)

var (
	ErrBucketNameMissing = errors.NewAppError(ERROR_MISSING_BUCKET_NAME)
	ErrFilePathMissing   = errors.NewAppError(ERROR_MISSING_FILE_PATH)
	ErrFileNameMissing   = errors.NewAppError(ERROR_MISSING_FILE_NAME)
)
