package router

import (
	"net/http"
	DB "oasisdb/internal/db"

	"github.com/gin-gonic/gin"
)

// NewRouter returns a new router
func New(db *DB.DB) *gin.Engine {
	engine := gin.Default()
	engine.POST("/collections", handleCreateCollection(db))
	engine.GET("/collections/:name", handleGetCollection(db))
	engine.DELETE("/collections/:name", handleDeleteCollection(db))
	engine.GET("/collections", handleListCollections(db))

	engine.POST("/collections/:name/documents", handleUpsertDocument(db))
	engine.GET("/collections/:name/documents/:id", handleGetDocument(db))
	engine.DELETE("/collections/:name/documents/:id", handleDeleteDocument(db))
	engine.POST("/collections/:name/documents/search", handleSearchDocuments(db))
	engine.POST("/collections/:name/documents/batch", handleBatchUpsertDocuments(db))
	engine.DELETE("/collections/:name/documents/batch", handleBatchDeleteDocuments(db))
	return engine
}

// Collection请求和响应结构
type CreateCollectionRequest struct {
	Name      string            `json:"name"`
	Dimension int               `json:"dimension"`
	Metadata  map[string]string `json:"metadata"`
}

// Document请求和响应结构
type UpsertDocumentRequest struct {
	ID       string                 `json:"id"`
	Vector   []float32              `json:"vector"`
	Metadata map[string]interface{} `json:"metadata"`
}

type SearchRequest struct {
	Vector []float32              `json:"vector"`
	Limit  int                    `json:"limit"`
	Filter map[string]interface{} `json:"filter"`
}

type BatchUpsertRequest struct {
	Documents []*DB.Document `json:"documents"`
}

type BatchDeleteRequest struct {
	IDs []string `json:"ids"`
}

// Collection相关处理函数
func handleCreateCollection(db *DB.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req CreateCollectionRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		collection, err := db.CreateCollection(req.Name, req.Dimension, req.Metadata)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusCreated, collection)
	}
}

func handleGetCollection(db *DB.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		name := c.Param("name")
		collection, err := db.GetCollection(name)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, collection)
	}
}

func handleDeleteCollection(db *DB.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		name := c.Param("name")
		if err := db.DeleteCollection(name); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.Status(http.StatusNoContent)
	}
}

func handleListCollections(db *DB.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		collections, err := db.ListCollections()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, collections)
	}
}

// Document相关处理函数
func handleUpsertDocument(db *DB.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		collectionName := c.Param("name")
		collection, err := db.GetCollection(collectionName)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}

		var req UpsertDocumentRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		doc := &DB.Document{
			ID:       req.ID,
			Vector:   req.Vector,
			Metadata: req.Metadata,
		}

		if err := collection.UpsertDocument(doc); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, doc)
	}
}

func handleGetDocument(db *DB.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		collectionName := c.Param("name")
		collection, err := db.GetCollection(collectionName)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}

		docID := c.Param("id")
		doc, err := collection.GetDocument(docID)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, doc)
	}
}

func handleDeleteDocument(db *DB.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		collectionName := c.Param("name")
		collection, err := db.GetCollection(collectionName)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}

		docID := c.Param("id")
		if err := collection.DeleteDocument(docID); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.Status(http.StatusNoContent)
	}
}

func handleSearchDocuments(db *DB.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		collectionName := c.Param("name")
		collection, err := db.GetCollection(collectionName)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}

		var req SearchRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		docs, distances, err := collection.SearchDocuments(req.Vector, req.Limit, req.Filter)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"documents": docs,
			"distances": distances,
		})
	}
}

func handleBatchUpsertDocuments(db *DB.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		collectionName := c.Param("name")
		collection, err := db.GetCollection(collectionName)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}

		var req BatchUpsertRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		if err := collection.BatchUpsertDocuments(req.Documents); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.Status(http.StatusOK)
	}
}

func handleBatchDeleteDocuments(db *DB.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		collectionName := c.Param("name")
		collection, err := db.GetCollection(collectionName)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}

		var req BatchDeleteRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		if err := collection.BatchDeleteDocuments(req.IDs); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.Status(http.StatusNoContent)
	}
}
