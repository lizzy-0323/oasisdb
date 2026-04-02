package storage

import (
	"testing"

	"oasisdb/internal/config"
	pkgerrors "oasisdb/pkg/errors"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStorageScalarOperations(t *testing.T) {
	conf, err := config.NewConfig(t.TempDir())
	require.NoError(t, err)

	storage, err := NewStorage(conf)
	require.NoError(t, err)
	t.Cleanup(storage.Stop)

	require.NoError(t, storage.PutScalar([]byte("alpha"), []byte("one")))

	value, exists, err := storage.GetScalar([]byte("alpha"))
	require.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, []byte("one"), value)

	require.NoError(t, storage.BatchPutScalar(
		[][]byte{[]byte("beta"), []byte("gamma")},
		[][]byte{[]byte("two"), []byte("three")},
	))

	value, exists, err = storage.GetScalar([]byte("gamma"))
	require.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, []byte("three"), value)

	require.NoError(t, storage.DeleteScalar([]byte("alpha")))

	value, exists, err = storage.GetScalar([]byte("alpha"))
	require.NoError(t, err)
	assert.True(t, exists)
	assert.Nil(t, value)
}

func TestStorageBatchPutScalarRejectsMismatchedLengths(t *testing.T) {
	conf, err := config.NewConfig(t.TempDir())
	require.NoError(t, err)

	storage, err := NewStorage(conf)
	require.NoError(t, err)
	t.Cleanup(storage.Stop)

	err = storage.BatchPutScalar(
		[][]byte{[]byte("alpha")},
		[][]byte{},
	)
	assert.ErrorIs(t, err, pkgerrors.ErrMisMatchKeysAndValues)
}
