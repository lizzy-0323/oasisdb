package server

import (
	"errors"
	"net/http"

	DB "oasisdb/internal/db"
	pkgerrors "oasisdb/pkg/errors"

	"github.com/gin-gonic/gin"
)

func (s *Server) handleHealthCheck() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	}
}

func (s *Server) handleSearchVectors() gin.HandlerFunc {
	return func(c *gin.Context) {
		collectionName := c.Param("name")
		var req SearchVectorRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		docs, distances, err := s.db.SearchVectors(collectionName, req.Vector, req.Limit)
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

func (s *Server) handleCreateCollection() gin.HandlerFunc {
	return func(c *gin.Context) {
		var req CreateCollectionRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		collection, err := s.db.CreateCollection(&DB.CreateCollectionOptions{
			Name:       req.Name,
			Dimension:  int(req.Dimension),
			Parameters: req.Parameters,
			IndexType:  req.IndexType,
		})
		if errors.Is(err, pkgerrors.ErrCollectionExists) {
			c.JSON(http.StatusOK, gin.H{"message": err.Error()})
			return
		}
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"name":      collection.Name,
			"dimension": collection.Dimension,
			"metadata":  collection.Metadata,
		})
	}
}

func (s *Server) handleGetCollection() gin.HandlerFunc {
	return func(c *gin.Context) {
		name := c.Param("name")
		collection, err := s.db.GetCollection(name)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"name":      collection.Name,
			"dimension": collection.Dimension,
			"metadata":  collection.Metadata,
		})
	}
}

func (s *Server) handleDeleteCollection() gin.HandlerFunc {
	return func(c *gin.Context) {
		name := c.Param("name")
		if err := s.db.DeleteCollection(name); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.Status(http.StatusOK)
	}
}

// TODO: ListCollections
func (s *Server) handleListCollections() gin.HandlerFunc {
	return func(c *gin.Context) {
		collections, err := s.db.ListCollections()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, collections)
	}
}

func (s *Server) handleUpsertDocument() gin.HandlerFunc {
	return func(c *gin.Context) {
		collectionName := c.Param("name")
		var req UpsertDocumentRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		doc := &DB.Document{
			ID:         req.ID,
			Vector:     req.Vector,
			Parameters: req.Parameters,
			Dimension:  int(len(req.Vector)),
		}

		if err := s.db.UpsertDocument(collectionName, doc); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"id":         doc.ID,
			"vector":     doc.Vector,
			"parameters": doc.Parameters,
			"dimension":  doc.Dimension,
		})
	}
}

func (s *Server) handleGetDocument() gin.HandlerFunc {
	return func(c *gin.Context) {
		collectionName := c.Param("name")
		docID := c.Param("id")

		doc, err := s.db.GetDocument(collectionName, docID)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"id":         doc.ID,
			"vector":     doc.Vector,
			"parameters": doc.Parameters,
			"dimension":  doc.Dimension,
		})
	}
}

func (s *Server) handleDeleteDocument() gin.HandlerFunc {
	return func(c *gin.Context) {
		collectionName := c.Param("name")
		docID := c.Param("id")
		if err := s.db.DeleteDocument(collectionName, docID); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.Status(http.StatusOK)
	}
}

func (s *Server) handleSearchDocuments() gin.HandlerFunc {
	return func(c *gin.Context) {
		collectionName := c.Param("name")
		var req SearchDocumentRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		docs, distances, err := s.db.SearchDocuments(collectionName, req.Vector, req.Limit, req.Filter)
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

func (s *Server) handleBatchUpsertDocuments() gin.HandlerFunc {
	return func(c *gin.Context) {
		collectionName := c.Param("name")
		var req BatchUpsertRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		if err := s.db.BatchUpsertDocuments(collectionName, req.Documents); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.Status(http.StatusOK)
	}
}

func (s *Server) Run(addr string) {
	s.router.Run(addr)
}
