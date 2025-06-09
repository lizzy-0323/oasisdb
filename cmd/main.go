package main

import (
	"oasisdb/internal/config"
	dblib "oasisdb/internal/db"
	"oasisdb/internal/server"
	"oasisdb/pkg/logger"
)

func main() {
	// Init Config
	conf, err := config.NewConfig(".")
	if err != nil {
		logger.Error("Failed to load config", "error", err)
		return
	}

	// Init DB
	db, err := dblib.New(conf)
	if err != nil {
		logger.Error("Failed to init database", "error", err)
		return
	}
	if err := db.Open(); err != nil {
		logger.Error("Failed to open database", "error", err)
		return
	}
	defer db.Close()

	// Init Server
	server := server.New(db)

	// Run Server
	server.Run(":8080")
}
