package main

import (
	"fmt"

	"oasisdb/internal/config"
	dblib "oasisdb/internal/db"
	"oasisdb/internal/server"
	"oasisdb/pkg/logger"
)

func printBanner() {
	fmt.Println(`
=================================================
   ___     _     ____  ___  ____   ____   ____  
  / _ \   / \   / ___||_ _|/ ___| |  _ \ | __ ) 
 | | | | / _ \  \___ \ | | \___ \ | | | ||  _ \ 
 | |_| |/ ___ \  ___) || |  ___) || |_| || |_) |
  \___//_/   \_\|____/|___||____/ |____/ |____/ 
=================================================
`)
}

func main() {
	// Init Config from file
	conf, err := config.FromFile("conf.yaml")
	if err != nil {
		logger.Error("Failed to load config from file", "error", err)
		return
	}

	// Initialize logger with config settings
	logger.InitLogger(conf.LogLevel, conf.LogFile)
	printBanner()
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
