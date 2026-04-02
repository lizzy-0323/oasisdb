package db

import (
	"fmt"
	"testing"

	"oasisdb/internal/config"
	pkgerrors "oasisdb/pkg/errors"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stubEmbeddingProvider struct {
	embedFn func(string) ([]float64, error)
}

func (s stubEmbeddingProvider) Embed(text string) ([]float64, error) {
	if s.embedFn == nil {
		return nil, fmt.Errorf("embed function is not configured")
	}
	return s.embedFn(text)
}

func (s stubEmbeddingProvider) EmbedBatch(texts []string) ([][]float64, error) {
	results := make([][]float64, len(texts))
	for i, text := range texts {
		embedding, err := s.Embed(text)
		if err != nil {
			return nil, err
		}
		results[i] = embedding
	}
	return results, nil
}

func newTestDBWithProvider(t *testing.T, provider stubEmbeddingProvider) *DB {
	t.Helper()

	conf, err := config.NewConfig(t.TempDir())
	require.NoError(t, err)
	conf.EmbeddingProvider = provider

	db, err := New(conf)
	require.NoError(t, err)
	require.NoError(t, db.Open())

	t.Cleanup(db.Close)
	return db
}

func createTestCollection(t *testing.T, db *DB, name string, dimension int) {
	t.Helper()

	_, err := db.CreateCollection(&CreateCollectionOptions{
		Name:       name,
		Dimension:  dimension,
		IndexType:  "hnsw",
		Parameters: map[string]string{},
	})
	require.NoError(t, err)
}

func TestDBSearchAndListCollections(t *testing.T) {
	db := newTestDBWithProvider(t, stubEmbeddingProvider{
		embedFn: func(text string) ([]float64, error) {
			return []float64{1, 0, 0}, nil
		},
	})

	createTestCollection(t, db, "docs", 3)
	require.NoError(t, db.BuildIndex("docs", []*Document{
		{ID: "1", Vector: []float32{1, 0, 0}, Parameters: map[string]any{"tag": "match"}},
		{ID: "2", Vector: []float32{0, 1, 0}, Parameters: map[string]any{"tag": "other"}},
		{ID: "3", Vector: []float32{0, 0, 1}, Parameters: map[string]any{"tag": "other"}},
	}))

	names, err := db.ListCollections()
	require.NoError(t, err)
	assert.Contains(t, names, "docs")

	ids, distances, err := db.SearchVectors("docs", []float32{1, 0, 0}, 2)
	require.NoError(t, err)
	require.NotEmpty(t, ids)
	assert.Equal(t, "1", ids[0])
	assert.Len(t, distances, len(ids))

	docs, docDistances, err := db.SearchDocuments("docs", &Document{
		Vector: []float32{1, 0, 0},
	}, 2, nil)
	require.NoError(t, err)
	require.NotEmpty(t, docs)
	assert.Equal(t, "1", docs[0].ID)
	assert.Len(t, docDistances, len(docs))
}

func TestDBEmbeddingAndSearchErrorPaths(t *testing.T) {
	db := newTestDBWithProvider(t, stubEmbeddingProvider{
		embedFn: func(text string) ([]float64, error) {
			if text == "boom" {
				return nil, fmt.Errorf("embedding failed")
			}
			return []float64{0.1, 0.2, 0.3}, nil
		},
	})

	createTestCollection(t, db, "docs", 3)

	require.NoError(t, db.UpsertDocument("docs", &Document{
		ID: "10",
		Parameters: map[string]any{
			"embedding": true,
			"text":      "hello",
		},
	}))

	doc, err := db.GetDocument("docs", "10")
	require.NoError(t, err)
	assert.Len(t, doc.Vector, 3)
	assert.Equal(t, 3, doc.Dimension)

	docs, distances, err := db.SearchDocuments("docs", &Document{
		Parameters: map[string]any{
			"embedding": true,
			"text":      "hello",
		},
	}, 1, nil)
	require.NoError(t, err)
	require.Len(t, docs, 1)
	assert.Equal(t, "10", docs[0].ID)
	assert.Len(t, distances, 1)

	_, _, err = db.SearchVectors("missing", []float32{0.1, 0.2, 0.3}, 1)
	assert.ErrorIs(t, err, pkgerrors.ErrCollectionNotFound)

	_, _, err = db.SearchDocuments("docs", &Document{}, 1, nil)
	assert.ErrorContains(t, err, "query document must have a vector")

	err = db.UpsertDocument("docs", &Document{
		ID: "11",
		Parameters: map[string]any{
			"embedding": true,
		},
	})
	assert.ErrorContains(t, err, "text parameter is required")

	_, _, err = db.SearchDocuments("docs", &Document{
		Parameters: map[string]any{
			"embedding": true,
		},
	}, 1, nil)
	assert.ErrorContains(t, err, "text parameter is required")

	_, _, err = db.SearchDocuments("docs", &Document{
		Parameters: map[string]any{
			"embedding": true,
			"text":      "boom",
		},
	}, 1, nil)
	assert.ErrorContains(t, err, "failed to generate embedding")

	err = db.UpsertDocument("docs", &Document{
		ID:        "12",
		Vector:    []float32{1, 2},
		Dimension: 3,
	})
	assert.ErrorContains(t, err, "vector dimension mismatch")
}

func TestDBBatchOperationsAndHelpers(t *testing.T) {
	db := newTestDBWithProvider(t, stubEmbeddingProvider{
		embedFn: func(text string) ([]float64, error) {
			switch text {
			case "batch":
				return []float64{0.5, 0.5}, nil
			default:
				return []float64{1, 0}, nil
			}
		},
	})

	createTestCollection(t, db, "batch_docs", 2)
	createTestCollection(t, db, "build_docs", 2)

	require.NoError(t, db.BatchUpsertDocuments("batch_docs", []*Document{
		{ID: "1", Vector: []float32{0, 1}, Parameters: map[string]any{"tag": "manual"}},
		{ID: "2", Parameters: map[string]any{"embedding": true, "text": "batch"}},
	}))

	doc, err := db.GetDocument("batch_docs", "2")
	require.NoError(t, err)
	assert.Equal(t, []float32{0.5, 0.5}, doc.Vector)
	assert.Equal(t, 2, doc.Dimension)

	prepared, err := db.prepareBatchData("batch_docs", []*Document{
		{ID: "3", Vector: []float32{1, 1}, Parameters: map[string]any{"ok": true}},
	})
	require.NoError(t, err)
	require.Len(t, prepared.ids, 1)
	assert.Equal(t, "3", prepared.ids[0])
	assert.Equal(t, []float32{1, 1}, prepared.vectors[0])
	assert.NotEmpty(t, prepared.docKeys[0])
	assert.NotEmpty(t, prepared.docValues[0])

	_, err = db.prepareBatchData("batch_docs", []*Document{
		{ID: "4", Vector: []float32{1}},
	})
	assert.ErrorContains(t, err, "vector dimension mismatch")

	_, err = db.prepareBatchData("batch_docs", []*Document{
		{ID: "5", Parameters: map[string]any{"embedding": true}},
	})
	assert.ErrorContains(t, err, "text parameter is required")

	require.NoError(t, db.BuildIndex("build_docs", []*Document{
		{ID: "6", Vector: []float32{1, 0}},
		{ID: "7", Vector: []float32{0, 1}},
	}))

	ids, _, err := db.SearchVectors("build_docs", []float32{1, 0}, 1)
	require.NoError(t, err)
	require.Len(t, ids, 1)
	assert.Equal(t, "6", ids[0])

	assert.Equal(t, []float32{1.5, -2.25}, float64SliceTo32([]float64{1.5, -2.25}))
}
