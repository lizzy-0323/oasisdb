package server

import (
	DB "oasisdb/internal/db"

	"github.com/gin-gonic/gin"
)

type Server struct {
	router *gin.Engine
	db     *DB.DB
}

// New creates a new server instance
func New(db *DB.DB) *Server {
	s := &Server{
		db:     db,
		router: gin.Default(),
	}
	s.setupRoutes()
	return s
}

func (s *Server) setupRoutes() {
	s.router.GET("/", s.handleHealthCheck())
	s.router.GET("/v1/collections/:name", s.handleGetCollection())
	s.router.DELETE("/v1/collections/:name", s.handleDeleteCollection())
	s.router.POST("/v1/collections/:name/buildindex", s.handleBuildIndex())
	s.router.POST("/v1/collections", s.handleCreateCollection())
	s.router.GET("/v1/collections", s.handleListCollections())

	s.router.POST("/v1/collections/:name/documents", s.handleUpsertDocument())
	s.router.POST("/v1/collections/:name/documents/setparams", s.handleSetParams())
	s.router.GET("/v1/collections/:name/documents/:id", s.handleGetDocument())
	s.router.DELETE("/v1/collections/:name/documents/:id", s.handleDeleteDocument())
	s.router.POST("/v1/collections/:name/vectors/search", s.handleSearchVectors())
	s.router.POST("/v1/collections/:name/documents/search", s.handleSearchDocuments())
	s.router.POST("/v1/collections/:name/documents/batchupsert", s.handleBatchUpsertDocuments())
}
