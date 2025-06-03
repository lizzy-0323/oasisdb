package main

import (
	"oasisdb/internal/config"
	"oasisdb/internal/db"
	"oasisdb/internal/router"
	"oasisdb/pkg/logger"
	"os"
	"path"
)

func main() {
	// Init Config
	conf, err := config.NewConfig(".")
	if err != nil {
		logger.Error("Failed to load config", "error", err)
		return
	}

	// Create data directory if not exists
	if err := os.MkdirAll(conf.Dir, 0755); err != nil {
		logger.Error("Failed to create data directory", "error", err)
		return
	}
	// Create WAL directory if not exists
	if err := os.MkdirAll(path.Join(conf.Dir, "walfile"), 0755); err != nil {
		logger.Error("Failed to create WAL directory", "error", err)
	}
	// Create index directory if not exists
	if err := os.MkdirAll(path.Join(conf.Dir, "indexfile"), 0755); err != nil {
		logger.Error("Failed to create index directory", "error", err)
	}
	// Create SST directory if not exists
	if err := os.MkdirAll(path.Join(conf.Dir, "sstfile"), 0755); err != nil {
		logger.Error("Failed to create SST directory", "error", err)
	}

	// Init DB
	db := &db.DB{}
	if err := db.Open(conf); err != nil {
		logger.Error("Failed to open database", "error", err)
		return
	}
	defer db.Close()

	// Init Router
	server := router.New(db)

	// Run Server
	server.Run(":8080")
}
