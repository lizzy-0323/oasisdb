package main

import (
	"oasisdb/internal/router"
)

func main() {
	r := router.NewRouter()
	r.Run(":8080")
}
