package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// OasisDB Go SDK
//
// This module provides a thin, high-level wrapper around the OasisDB HTTP API so that
// users can interact with the database easily from Go code.
//
// All methods raise OasisDBError when the server returns a non-successful status code.
//
// Example usage:
//  client := NewOasisDBClient("http://localhost:8080")
//  ok, err := client.HealthCheck()
//  ...

// OasisDBClient is a high-level HTTP client for OasisDB.
// Provides health check, collection management, document management, vector search, etc.
type OasisDBClient struct {
	BaseURL string
	Client  *http.Client
}

// OasisDBError represents an error returned by the OasisDB server.
type OasisDBError struct {
	StatusCode int
	Message    string
}

func (e *OasisDBError) Error() string {
	return fmt.Sprintf("OasisDBError: %d %s", e.StatusCode, e.Message)
}

// NewOasisDBClient creates a new OasisDB client.
func NewOasisDBClient(baseURL string) *OasisDBClient {
	return &OasisDBClient{
		BaseURL: baseURL,
		Client:  &http.Client{Timeout: 30 * time.Second},
	}
}

// ----------------- Low-level request helper -----------------
// request sends an HTTP request and returns the response body.
func (c *OasisDBClient) request(method, path string, body any) ([]byte, error) {
	url := c.BaseURL + path
	var reqBody io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reqBody = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, reqBody)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= http.StatusBadRequest {
		return nil, &OasisDBError{StatusCode: resp.StatusCode, Message: string(respBody)}
	}
	return respBody, nil
}

// ----------------- API Methods -----------------

// HealthCheck checks if the server is healthy. Returns true if healthy.
func (c *OasisDBClient) HealthCheck() (bool, error) {
	resp, err := c.request("GET", "/", nil)
	if err != nil {
		return false, err
	}
	var result map[string]any
	if err := json.Unmarshal(resp, &result); err != nil {
		return false, err
	}
	return result["status"] == "ok", nil
}

// CreateCollection creates a new collection.
func (c *OasisDBClient) CreateCollection(name string, dimension int, indexType string, parameters map[string]any) (map[string]any, error) {
	payload := map[string]any{
		"name":       name,
		"dimension":  dimension,
		"index_type": indexType,
		"parameters": parameters,
	}
	resp, err := c.request("POST", "/v1/collections", payload)
	if err != nil {
		return nil, err
	}
	var result map[string]any
	err = json.Unmarshal(resp, &result)
	return result, err
}

// GetCollection retrieves collection information.
func (c *OasisDBClient) GetCollection(name string) (map[string]any, error) {
	resp, err := c.request("GET", "/v1/collections/"+name, nil)
	if err != nil {
		return nil, err
	}
	var result map[string]any
	err = json.Unmarshal(resp, &result)
	return result, err
}

// ListCollections lists all collections.
func (c *OasisDBClient) ListCollections() ([]map[string]any, error) {
	resp, err := c.request("GET", "/v1/collections", nil)
	if err != nil {
		return nil, err
	}
	var result []map[string]any
	err = json.Unmarshal(resp, &result)
	return result, err
}

// DeleteCollection deletes a collection.
func (c *OasisDBClient) DeleteCollection(name string) error {
	_, err := c.request("DELETE", "/v1/collections/"+name, nil)
	return err
}

// UpsertDocument inserts or updates a document.
func (c *OasisDBClient) UpsertDocument(collection, docID string, vector []float32, parameters map[string]interface{}) (map[string]any, error) {
	if collection == "" || docID == "" || len(vector) == 0 {
		return nil, fmt.Errorf("collection, docID, and vector must not be empty")
	}
	payload := map[string]any{
		"id":     docID,
		"vector": vector,
	}
	if parameters != nil {
		payload["parameters"] = parameters
	}
	resp, err := c.request("POST", fmt.Sprintf("/v1/collections/%s/documents", collection), payload)
	if err != nil {
		return nil, fmt.Errorf("upsert document failed: %w", err)
	}
	var result map[string]any
	err = json.Unmarshal(resp, &result)
	return result, err
}

// CountDocuments returns the number of documents in a collection.
func (c *OasisDBClient) CountDocuments(collection string) (int, error) {
	resp, err := c.request("GET", fmt.Sprintf("/v1/collections/%s/documents/count", collection), nil)
	if err != nil {
		return 0, err
	}
	var result map[string]int
	err = json.Unmarshal(resp, &result)
	if err != nil {
		return 0, err
	}
	return result["count"], nil
}

// BatchUpsertDocuments inserts or updates multiple documents in batch.
func (c *OasisDBClient) BatchUpsertDocuments(collection string, documents []map[string]any) error {
	payload := map[string]any{"documents": documents}
	_, err := c.request("POST", fmt.Sprintf("/v1/collections/%s/documents/batchupsert", collection), payload)
	return err
}

// GetDocument retrieves a document.
func (c *OasisDBClient) GetDocument(collection, docID string) (map[string]any, error) {
	resp, err := c.request("GET", fmt.Sprintf("/v1/collections/%s/documents/%s", collection, docID), nil)
	if err != nil {
		return nil, err
	}
	var result map[string]any
	err = json.Unmarshal(resp, &result)
	return result, err
}

// DeleteDocument deletes a document.
func (c *OasisDBClient) DeleteDocument(collection, docID string) error {
	_, err := c.request("DELETE", fmt.Sprintf("/v1/collections/%s/documents/%s", collection, docID), nil)
	return err
}

// BuildIndex builds the index for a collection.
func (c *OasisDBClient) BuildIndex(collection string, documents []map[string]any) error {
	payload := map[string]any{"documents": documents}
	_, err := c.request("POST", fmt.Sprintf("/v1/collections/%s/buildindex", collection), payload)
	return err
}

// SetParams sets collection parameters.
func (c *OasisDBClient) SetParams(collection string, parameters map[string]any) error {
	payload := map[string]any{"parameters": parameters}
	_, err := c.request("POST", fmt.Sprintf("/v1/collections/%s/documents/setparams", collection), payload)
	return err
}

// SearchVectors performs a vector search.
func (c *OasisDBClient) SearchVectors(collection string, vector []float32, limit int) (map[string]any, error) {
	payload := map[string]any{"vector": vector, "limit": limit}
	resp, err := c.request("POST", fmt.Sprintf("/v1/collections/%s/vectors/search", collection), payload)
	if err != nil {
		return nil, err
	}
	var result map[string]any
	err = json.Unmarshal(resp, &result)
	return result, err
}

// SearchDocuments performs a document search.
func (c *OasisDBClient) SearchDocuments(collection string, vector []float32, limit int, filter map[string]any) (map[string]any, error) {
	payload := map[string]any{"vector": vector, "limit": limit}
	if filter != nil {
		payload["filter"] = filter
	}
	resp, err := c.request("POST", fmt.Sprintf("/v1/collections/%s/documents/search", collection), payload)
	if err != nil {
		return nil, err
	}
	var result map[string]any
	err = json.Unmarshal(resp, &result)
	return result, err
}
