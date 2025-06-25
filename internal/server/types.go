package server

import DB "oasisdb/internal/db"

// CreateCollectionRequest represents the request body for creating a collection
type CreateCollectionRequest struct {
	Name       string            `json:"name"`
	Dimension  uint32            `json:"dimension"`
	IndexType  string            `json:"index_type"`
	Parameters map[string]string `json:"parameters,omitempty"`
}

// GetCollectionResponse represents the response body for getting a collection
type GetCollectionResponse struct {
	Name      string `json:"name"`
	Dimension uint32 `json:"dimension"`
}

// ListCollectionsResponse represents the response body for listing collections
type ListCollectionsResponse struct {
	Collections []GetCollectionResponse `json:"collections"`
}

// UpsertDocumentRequest represents the request body for upserting a document
type UpsertDocumentRequest struct {
	ID         string                 `json:"id"`
	Vector     []float32              `json:"vector"`
	Parameters map[string]interface{} `json:"parameters,omitempty"`
}

// SearchRequest represents the request body for searching documents
type SearchRequest struct {
	Vector []float32 `json:"vector"`
	TopK   int       `json:"top_k"`
}

// SearchResponse represents the response body for search results
type SearchResponse struct {
	Results []SearchResult `json:"results"`
}

// SearchResult represents a single search result
type SearchResult struct {
	ID       string  `json:"id"`
	Score    float32 `json:"score"`
	Distance float32 `json:"distance"`
}

type SearchDocumentRequest struct {
	Vector []float32      `json:"vector"`
	Limit  int            `json:"limit"`
	Filter map[string]any `json:"filter"`
}

type SetParamsRequest struct {
	Parameters map[string]any `json:"parameters"`
}
type SearchVectorRequest struct {
	Vector []float32 `json:"vector"`
	Limit  int       `json:"limit"`
}

type BatchUpsertRequest struct {
	Documents []*DB.Document `json:"documents"`
}

type BatchDeleteRequest struct {
	IDs []string `json:"ids"`
}
