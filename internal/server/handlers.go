package server

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	DB "oasisdb/internal/db"
	pkgerrors "oasisdb/pkg/errors"

	"github.com/gin-gonic/gin"
)

// generateCacheKey creates a unique key for caching search results
func generateCacheKey(collection string, vector []float32, limit int) string {
	// Convert parameters to a string representation
	vectorBytes, _ := json.Marshal(vector)

	// Combine all parameters into a single string
	data := fmt.Sprintf("%s:%s:%d", collection, string(vectorBytes), limit)

	// Generate SHA-256 hash
	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:])
}

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

		// Generate cache key
		cacheKey := generateCacheKey(collectionName, req.Vector, req.Limit)

		// Try to get from cache first
		if cachedResult, exists := s.db.Cache.Get(cacheKey); exists {
			result := cachedResult.(gin.H)
			result["other"] = "cache_hit"
			c.JSON(http.StatusOK, result)
			return
		}

		ids, distances, err := s.db.SearchVectors(collectionName, req.Vector, req.Limit)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		// Prepare response
		response := gin.H{
			"ids":       ids,
			"distances": distances,
		}

		// Cache the result
		s.db.Cache.Set(cacheKey, response)

		// Return response
		c.JSON(http.StatusOK, response)
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

func (s *Server) handleBuildIndex() gin.HandlerFunc {
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

		doc, err := s.db.GetDocument(collectionName, docID)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}

		if err := s.db.DeleteDocument(collectionName, docID); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		if s.db.Cache != nil {
			// Delete all cached entries for this document's vector
			prefix := fmt.Sprintf("%s:%v", collectionName, doc.Vector)
			s.db.Cache.DeleteWithPrefix(prefix)
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

        // Create query document from request
        queryDoc := &DB.Document{
            Vector:    req.Vector,
            Dimension: len(req.Vector),
        }

        // Call SearchDocuments with query document and correct field names
        results, distances, err := s.db.SearchDocuments(collectionName, queryDoc, req.Limit, req.Filter)
        if err != nil {
            c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
            return
        }

        // Convert results to response format
        docs := make([]map[string]interface{}, len(results))
        for i, doc := range results {
            docs[i] = map[string]interface{}{
                "id":         doc.ID,
                "vector":     doc.Vector,
                "parameters": doc.Parameters,
                "dimension":  doc.Dimension,
                "distance":   distances[i],
            }
        }

        // Prepare response
        response := gin.H{
            "documents": docs,
            "distances": distances,
        }

        c.JSON(http.StatusOK, response)
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

// handleSetParams adjusts search parameters for a collection's vector index.
// Currently supported parameters:
//   - efsearch : HNSW indices (improves recall at the cost of speed)
//   - nprobe   : IVF indices  (controls the number of inverted lists scanned)
func (s *Server) handleSetParams() gin.HandlerFunc {
	return func(c *gin.Context) {
		collectionName := c.Param("name")

		var req SetParamsRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		idx, err := s.db.IndexManager.GetIndex(collectionName)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		if err := idx.SetParams(req.Parameters); err != nil {
			if errors.Is(err, pkgerrors.ErrInvalidParameter) {
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			} else {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			}
			return
		}

		c.Status(http.StatusOK)
	}
}

func (s *Server) Run(addr string) {
	s.router.Run(addr)
}
