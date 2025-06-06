package server

import (
	DB "oasisdb/internal/db"

	"github.com/gin-gonic/gin"
)

type Server struct {
	router *gin.Engine
	db     *DB.DB
}

// Collection请求和响应结构
type CreateCollectionRequest struct {
	Name       string            `json:"name"`
	Dimension  int               `json:"dimension"`
	Parameters map[string]string `json:"parameters"`
}

// Document请求和响应结构
type UpsertDocumentRequest struct {
	ID         string                 `json:"id"`
	Vector     []float32              `json:"vector"`
	Parameters map[string]interface{} `json:"parameters"`
}

type SearchDocumentRequest struct {
	Vector []float32              `json:"vector"`
	Limit  int                    `json:"limit"`
	Filter map[string]interface{} `json:"filter"`
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

// New returns a new router
func New(db *DB.DB) *Server {
	router := gin.Default()
	s := &Server{
		router: router,
		db:     db,
	}
	s.router.GET("/", s.handleHealthCheck())
	s.router.POST("/v1/collections/:name/search", s.handleSearch())
	s.router.POST("/v1/collections", s.handleCreateCollection())
	s.router.GET("/v1/collections/:name", s.handleGetCollection())
	s.router.DELETE("/v1/collections/:name", s.handleDeleteCollection())
	s.router.GET("/v1/collections", s.handleListCollections())

	s.router.POST("/v1/collections/:name/documents", s.handleUpsertDocument())
	s.router.GET("/v1/collections/:name/documents/:id", s.handleGetDocument())
	s.router.DELETE("/v1/collections/:name/documents/:id", s.handleDeleteDocument())
	s.router.POST("/v1/collections/:name/documents/search", s.handleSearchDocuments())
	s.router.POST("/v1/collections/:name/documents/batch", s.handleBatchUpsertDocuments())
	s.router.DELETE("/v1/collections/:name/documents/batch", s.handleBatchDeleteDocuments())
	return s
}
