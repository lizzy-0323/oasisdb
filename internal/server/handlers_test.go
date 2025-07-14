package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"oasisdb/internal/config"
	"oasisdb/internal/db"
	"oasisdb/internal/index"

	"github.com/stretchr/testify/assert"
)

func setupTestServer(t *testing.T) (*Server, func()) {
	// Create a temporary directory for testing
	tmpDir, err := os.MkdirTemp("", "oasisdb_test_*")
	assert.NoError(t, err)

	// Create config
	conf, err := config.NewConfig(tmpDir)
	assert.NoError(t, err)

	// Create DB
	db, err := db.New(conf)
	assert.NoError(t, err)
	err = db.Open()
	assert.NoError(t, err)

	// Create server
	server := New(db)
	assert.NotNil(t, server)

	// Return cleanup function
	cleanup := func() {
		db.Close()
		os.RemoveAll(tmpDir)
	}

	return server, cleanup
}

func TestHandleCreateCollection(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Test successful creation
	req := CreateCollectionRequest{
		Name:      "test_collection",
		Dimension: 128,
		Parameters: map[string]string{
			"M":              "16",
			"efConstruction": "100",
		},
	}

	body, err := json.Marshal(req)
	assert.NoError(t, err)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/collections", bytes.NewReader(body))
	server.router.ServeHTTP(w, r)

	assert.Equal(t, http.StatusOK, w.Code)

	// Test duplicate creation
	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodPost, "/v1/collections", bytes.NewReader(body))
	server.router.ServeHTTP(w, r)

	assert.Equal(t, http.StatusOK, w.Code)

	// Test invalid request
	invalidReq := CreateCollectionRequest{
		Name: "test_collection", // Missing required fields
	}
	body, err = json.Marshal(invalidReq)
	assert.NoError(t, err)

	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodPost, "/v1/collections", bytes.NewReader(body))
	server.router.ServeHTTP(w, r)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestHandleGetCollection(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Create a collection first
	req := CreateCollectionRequest{
		Name:      "test_collection",
		Dimension: 128,
	}

	body, err := json.Marshal(req)
	assert.NoError(t, err)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/collections", bytes.NewReader(body))
	server.router.ServeHTTP(w, r)

	assert.Equal(t, http.StatusOK, w.Code)

	// Test get existing collection
	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodGet, "/v1/collections/test_collection", nil)
	server.router.ServeHTTP(w, r)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp GetCollectionResponse
	err = json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, req.Name, resp.Name)
	assert.Equal(t, req.Dimension, resp.Dimension)

	// Test get non-existent collection
	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodGet, "/v1/collections/non_existent", nil)
	server.router.ServeHTTP(w, r)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestHandleDeleteCollection(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Create a collection first
	req := CreateCollectionRequest{
		Name:      "test_collection",
		Dimension: 128,
	}

	body, err := json.Marshal(req)
	assert.NoError(t, err)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/collections", bytes.NewReader(body))
	server.router.ServeHTTP(w, r)

	assert.Equal(t, http.StatusOK, w.Code)

	// Test delete existing collection
	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodDelete, "/v1/collections/test_collection", nil)
	server.router.ServeHTTP(w, r)

	assert.Equal(t, http.StatusOK, w.Code)

	// Verify collection is deleted
	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodGet, "/v1/collections/test_collection", nil)
	server.router.ServeHTTP(w, r)

	assert.Equal(t, http.StatusNotFound, w.Code)

	// Test delete non-existent collection
	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodDelete, "/v1/collections/non_existent", nil)
	server.router.ServeHTTP(w, r)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestHandleUpsertDocument(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Create a collection first
	collReq := CreateCollectionRequest{
		Name:      "test_collection",
		Dimension: 3,
	}

	body, err := json.Marshal(collReq)
	assert.NoError(t, err)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/collections", bytes.NewReader(body))
	server.router.ServeHTTP(w, r)

	assert.Equal(t, http.StatusOK, w.Code)

	// Test successful upsert
	docReq := UpsertDocumentRequest{
		ID:     "doc1",
		Vector: []float32{1.0, 2.0, 3.0},
		Parameters: map[string]interface{}{
			"tag": "test",
		},
	}

	body, err = json.Marshal(docReq)
	assert.NoError(t, err)

	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodPost, "/v1/collections/test_collection/documents", bytes.NewReader(body))
	server.router.ServeHTTP(w, r)

	assert.Equal(t, http.StatusOK, w.Code)

	// Test invalid vector dimension
	invalidReq := UpsertDocumentRequest{
		ID:     "doc2",
		Vector: []float32{1.0, 2.0}, // Wrong dimension
	}

	body, err = json.Marshal(invalidReq)
	assert.NoError(t, err)

	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodPost, "/v1/collections/test_collection/documents", bytes.NewReader(body))
	server.router.ServeHTTP(w, r)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	t.Log(w.Body.String())

	// Test non-existent collection
	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodPost, "/v1/collections/non_existent/documents", bytes.NewReader(body))
	server.router.ServeHTTP(w, r)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	t.Log(w.Body.String())
}

func TestHandleGetDocument(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Create a collection first
	collReq := CreateCollectionRequest{
		Name:      "test_collection",
		Dimension: 3,
	}

	body, err := json.Marshal(collReq)
	assert.NoError(t, err)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/collections", bytes.NewReader(body))
	server.router.ServeHTTP(w, r)

	assert.Equal(t, http.StatusOK, w.Code)

	// Create a document
	docReq := UpsertDocumentRequest{
		ID:     "doc1",
		Vector: []float32{1.0, 2.0, 3.0},
		Parameters: map[string]interface{}{
			"tag": "test",
		},
	}

	body, err = json.Marshal(docReq)
	assert.NoError(t, err)

	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodPost, "/v1/collections/test_collection/documents", bytes.NewReader(body))
	server.router.ServeHTTP(w, r)

	assert.Equal(t, http.StatusOK, w.Code)

	// Test get existing document
	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodGet, "/v1/collections/test_collection/documents/doc1", nil)
	server.router.ServeHTTP(w, r)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]interface{}
	err = json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, docReq.ID, resp["id"])

	// Test get non-existent document
	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodGet, "/v1/collections/test_collection/documents/non_existent", nil)
	server.router.ServeHTTP(w, r)

	assert.Equal(t, http.StatusNotFound, w.Code)

	// Test get document from non-existent collection
	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodGet, "/v1/collections/non_existent/documents/doc1", nil)
	server.router.ServeHTTP(w, r)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestHandleDeleteDocument(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Create a collection first
	collReq := CreateCollectionRequest{
		Name:      "test_collection",
		Dimension: 3,
	}

	body, err := json.Marshal(collReq)
	assert.NoError(t, err)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/collections", bytes.NewReader(body))
	server.router.ServeHTTP(w, r)

	assert.Equal(t, http.StatusOK, w.Code)

	// Create a document
	docReq := UpsertDocumentRequest{
		ID:     "doc1",
		Vector: []float32{1.0, 2.0, 3.0},
	}

	body, err = json.Marshal(docReq)
	assert.NoError(t, err)

	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodPost, "/v1/collections/test_collection/documents", bytes.NewReader(body))
	server.router.ServeHTTP(w, r)

	assert.Equal(t, http.StatusOK, w.Code)

	// Test delete existing document
	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodDelete, "/v1/collections/test_collection/documents/doc1", nil)
	server.router.ServeHTTP(w, r)

	assert.Equal(t, http.StatusOK, w.Code)

	// Verify document is deleted
	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodGet, "/v1/collections/test_collection/documents/doc1", nil)
	server.router.ServeHTTP(w, r)

	assert.Equal(t, http.StatusNotFound, w.Code)

	// Test delete non-existent document
	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodDelete, "/v1/collections/test_collection/documents/non_existent", nil)
	server.router.ServeHTTP(w, r)

	assert.Equal(t, http.StatusNotFound, w.Code)
	t.Log(w.Body.String())

	// Test delete document from non-existent collection
	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodDelete, "/v1/collections/non_existent/documents/doc1", nil)
	server.router.ServeHTTP(w, r)

	assert.Equal(t, http.StatusNotFound, w.Code)
	t.Log(w.Body.String())
}

func TestHandleSearchDocuments(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Create a collection first
	collReq := CreateCollectionRequest{
		Name:      "test_collection",
		Dimension: 3,
		Parameters: map[string]string{
			"space":             "l2",
			"index_type":        "hnsw",
			"M":                 "16",
			"ef":                "100",
			"efConstruction":    "100",
			"max_elements":      "1000",
			"similarity_metric": "l2",
		},
	}

	body, err := json.Marshal(collReq)
	assert.NoError(t, err)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/collections", bytes.NewReader(body))
	server.router.ServeHTTP(w, r)

	assert.Equal(t, http.StatusOK, w.Code)

	// Create some documents
	docs := []UpsertDocumentRequest{
		{
			ID:     "1",
			Vector: []float32{1.0, 2.0, 3.0},
			Parameters: map[string]interface{}{
				"tag": "test1",
			},
		},
		{
			ID:     "2",
			Vector: []float32{4.0, 5.0, 6.0},
			Parameters: map[string]interface{}{
				"tag": "test2",
			},
		},
	}

	for _, doc := range docs {
		body, err = json.Marshal(doc)
		assert.NoError(t, err)

		w = httptest.NewRecorder()
		r = httptest.NewRequest(http.MethodPost, "/v1/collections/test_collection/documents", bytes.NewReader(body))
		server.router.ServeHTTP(w, r)

		assert.Equal(t, http.StatusOK, w.Code)
	}

	// Test search documents
	searchReq := SearchDocumentRequest{
		Vector: []float32{1.0, 2.0, 3.0},
		Limit:  2,
		Filter: map[string]interface{}{
			"tag": "test1",
		},
	}

	body, err = json.Marshal(searchReq)
	assert.NoError(t, err)

	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodPost, "/v1/collections/test_collection/documents/search", bytes.NewReader(body))
	server.router.ServeHTTP(w, r)

	assert.Equal(t, http.StatusOK, w.Code)
	// t.Log(w.Body.String())

	// Test invalid vector dimension
	invalidReq := SearchDocumentRequest{
		Vector: []float32{1.0, 2.0}, // Wrong dimension
		Limit:  2,
	}

	body, err = json.Marshal(invalidReq)
	assert.NoError(t, err)

	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodPost, "/v1/collections/test_collection/documents/search", bytes.NewReader(body))
	server.router.ServeHTTP(w, r)

	assert.Equal(t, http.StatusInternalServerError, w.Code)

	// Test search in non-existent collection
	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodPost, "/v1/collections/non_existent/documents/search", bytes.NewReader(body))
	server.router.ServeHTTP(w, r)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestHandleBuildIndex(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// create collection first
	collReq := CreateCollectionRequest{
		Name:      "test_collection",
		IndexType: string(index.IVFFLATIndex),
		Dimension: 3,
	}
	body, err := json.Marshal(collReq)
	assert.NoError(t, err)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/collections", bytes.NewReader(body))
	server.router.ServeHTTP(w, r)
	assert.Equal(t, http.StatusOK, w.Code)

	// prepare documents for build index
	docs := []*db.Document{
		{ID: "doc1", Vector: []float32{1.0, 2.0, 3.0}, Parameters: map[string]any{"tag": "a"}},
		{ID: "doc2", Vector: []float32{4.0, 5.0, 6.0}},
	}
	buildReq := BatchUpsertRequest{Documents: docs}
	body, err = json.Marshal(buildReq)
	assert.NoError(t, err)

	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodPost, "/v1/collections/test_collection/buildindex", bytes.NewReader(body))
	server.router.ServeHTTP(w, r)
	assert.Equal(t, http.StatusOK, w.Code)

	// invalid vector dimension
	docsInvalid := []*db.Document{{ID: "bad", Vector: []float32{1.0, 2.0}}} // dimension 2 != 3
	buildReq = BatchUpsertRequest{Documents: docsInvalid}
	body, err = json.Marshal(buildReq)
	assert.NoError(t, err)

	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodPost, "/v1/collections/test_collection/buildindex", bytes.NewReader(body))
	server.router.ServeHTTP(w, r)
	assert.Equal(t, http.StatusInternalServerError, w.Code)

	// non-existent collection
	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodPost, "/v1/collections/non_existent/buildindex", bytes.NewReader(body))
	server.router.ServeHTTP(w, r)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestHandleSearchVectors(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Create a collection first
	collReq := CreateCollectionRequest{
		Name:      "test_collection",
		Dimension: 3,
	}

	body, err := json.Marshal(collReq)
	assert.NoError(t, err)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/collections", bytes.NewReader(body))
	server.router.ServeHTTP(w, r)

	assert.Equal(t, http.StatusOK, w.Code)

	// Create some documents
	docs := []UpsertDocumentRequest{
		{
			ID:     "1",
			Vector: []float32{1.0, 2.0, 3.0},
			Parameters: map[string]interface{}{
				"tag": "test1",
			},
		},
		{
			ID:     "2",
			Vector: []float32{4.0, 5.0, 6.0},
			Parameters: map[string]interface{}{
				"tag": "test2",
			},
		},
	}

	for _, doc := range docs {
		body, err = json.Marshal(doc)
		assert.NoError(t, err)

		w = httptest.NewRecorder()
		r = httptest.NewRequest(http.MethodPost, "/v1/collections/test_collection/documents", bytes.NewReader(body))
		server.router.ServeHTTP(w, r)

		assert.Equal(t, http.StatusOK, w.Code)
	}

	// Test search vectors
	searchReq := SearchVectorRequest{
		Vector: []float32{1.0, 2.0, 3.0},
		Limit:  2,
	}

	body, err = json.Marshal(searchReq)
	assert.NoError(t, err)

	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodPost, "/v1/collections/test_collection/vectors/search", bytes.NewReader(body))
	server.router.ServeHTTP(w, r)
	assert.Equal(t, http.StatusOK, w.Code)

	// Test if lru enabled
	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodPost, "/v1/collections/test_collection/vectors/search", bytes.NewReader(body))
	server.router.ServeHTTP(w, r)

	assert.Equal(t, http.StatusOK, w.Code)
	// print result
	assert.Contains(t, w.Body.String(), "cache_hit")

	// Test invalid vector dimension
	invalidReq := SearchVectorRequest{
		Vector: []float32{1.0, 2.0}, // Wrong dimension
		Limit:  2,
	}

	body, err = json.Marshal(invalidReq)
	assert.NoError(t, err)

	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodPost, "/v1/collections/test_collection/vectors/search", bytes.NewReader(body))
	server.router.ServeHTTP(w, r)

	assert.Equal(t, http.StatusInternalServerError, w.Code)

	// Test search in non-existent collection
	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodPost, "/v1/collections/non_existent/vectors/search", bytes.NewReader(body))
	server.router.ServeHTTP(w, r)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestHandleSetParams(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// ---------- HNSW collection ----------
	hnswReq := CreateCollectionRequest{
		Name:      "hnsw_coll",
		IndexType: "hnsw",
		Dimension: 3,
	}
	body, err := json.Marshal(hnswReq)
	assert.NoError(t, err)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/collections", bytes.NewReader(body))
	server.router.ServeHTTP(w, r)
	assert.Equal(t, http.StatusOK, w.Code)

	// valid efsearch param
	paramReq := SetParamsRequest{Parameters: map[string]any{"efsearch": 64}}
	body, err = json.Marshal(paramReq)
	assert.NoError(t, err)
	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodPost, "/v1/collections/hnsw_coll/documents/setparams", bytes.NewReader(body))
	server.router.ServeHTTP(w, r)
	assert.Equal(t, http.StatusOK, w.Code)

	// invalid nprobe on hnsw (should fail)
	paramReq = SetParamsRequest{Parameters: map[string]any{"nprobe": 5}}
	body, _ = json.Marshal(paramReq)
	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodPost, "/v1/collections/hnsw_coll/documents/setparams", bytes.NewReader(body))
	server.router.ServeHTTP(w, r)
	assert.Equal(t, http.StatusBadRequest, w.Code)

	// ---------- IVF collection ----------
	ivfReq := CreateCollectionRequest{
		Name:      "ivf_coll",
		IndexType: string(index.IVFFLATIndex),
		Dimension: 3,
	}
	body, err = json.Marshal(ivfReq)
	assert.NoError(t, err)
	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodPost, "/v1/collections", bytes.NewReader(body))
	server.router.ServeHTTP(w, r)
	assert.Equal(t, http.StatusOK, w.Code)

	// valid nprobe param
	paramReq = SetParamsRequest{Parameters: map[string]any{"nprobe": 20}}
	body, _ = json.Marshal(paramReq)
	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodPost, "/v1/collections/ivf_coll/documents/setparams", bytes.NewReader(body))
	server.router.ServeHTTP(w, r)
	assert.Equal(t, http.StatusOK, w.Code)

	// invalid efsearch on ivf (should fail)
	paramReq = SetParamsRequest{Parameters: map[string]any{"efsearch": 128}}
	body, _ = json.Marshal(paramReq)
	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodPost, "/v1/collections/ivf_coll/documents/setparams", bytes.NewReader(body))
	server.router.ServeHTTP(w, r)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}
