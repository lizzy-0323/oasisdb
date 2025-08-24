package main

import (
	"oasisdb/internal/config"
	dblib "oasisdb/internal/db"
	"oasisdb/internal/server"
	"oasisdb/pkg/logger"
)

func main() {
	// Init Config from file
	conf, err := config.FromFile("conf.yaml")
	if err != nil {
		logger.Error("Failed to load config from file", "error", err)
		return
	}

	// Initialize logger with config settings
	logger.InitLogger(conf.LogLevel, conf.LogFile)
	logger.Info("OasisDB starting", "log_level", conf.LogLevel, "log_file", conf.LogFile)

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
