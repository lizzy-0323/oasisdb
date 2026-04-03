package errors

import "testing"

func TestExportedErrorsHaveStableMessages(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"ErrCollectionExists", ErrCollectionExists, "collection already exists"},
		{"ErrCollectionNotFound", ErrCollectionNotFound, "collection not found"},
		{"ErrDocumentNotFound", ErrDocumentNotFound, "document not found"},
		{"ErrDocumentExists", ErrDocumentExists, "document already exists"},
		{"ErrNoResultsFound", ErrNoResultsFound, "no satisfied results found"},
		{"ErrIndexNotFound", ErrIndexNotFound, "index not found"},
		{"ErrInvalidDimension", ErrInvalidDimension, "invalid vector dimension"},
		{"ErrFailedToCreateIndex", ErrFailedToCreateIndex, "failed to create index"},
		{"ErrFailedToLoadIndex", ErrFailedToLoadIndex, "failed to load index"},
		{"ErrUnsupportedIndexType", ErrUnsupportedIndexType, "unsupported index type"},
		{"ErrMisMatchKeysAndValues", ErrMisMatchKeysAndValues, "keys and values length mismatch"},
		{"ErrInvalidParameter", ErrInvalidParameter, "invalid parameter"},
		{"ErrEmptyParameter", ErrEmptyParameter, "empty parameter"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.err == nil {
				t.Fatal("expected error to be initialized")
			}
			if tt.err.Error() != tt.want {
				t.Fatalf("expected %q, got %q", tt.want, tt.err.Error())
			}
		})
	}
}
