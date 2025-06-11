package errors

import "errors"

var (
	// Collection errors
	ErrCollectionExists   = errors.New("collection already exists")
	ErrCollectionNotFound = errors.New("collection not found")

	// Document errors
	ErrDocumentNotFound = errors.New("document not found")
	ErrDocumentExists   = errors.New("document already exists")

	// Index errors
	ErrIndexNotFound        = errors.New("index not found")
	ErrNotImplemented       = errors.New("not implemented")
	ErrInvalidDimension     = errors.New("invalid vector dimension")
	ErrFailedToCreateIndex  = errors.New("failed to create index")
	ErrFailedToLoadIndex    = errors.New("failed to load index")
	ErrUnsupportedIndexType = errors.New("unsupported index type")

	// Storage errors
	ErrMisMatchKeysAndValues = errors.New("keys and values length mismatch")
)
