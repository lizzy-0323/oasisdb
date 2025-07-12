package main
/*
Example script for OasisDB Go SDK.

Run this after the OasisDB server has started (default address: http://localhost:8080).
It will:
  1. Perform a health-check.
  2. Create a collection called 'demo'.
  3. Upsert a few documents.
  4. Perform a vector and a document search.
  5. Clean up by deleting the collection.

Usage:
$ go run example.go
*/

import (
	"fmt"
	"math/rand"
)

func randomVector(dim int) []float32 {
	v := make([]float32, dim)
	for i := range v {
		v[i] = rand.Float32()
	}
	return v
}

func main() {
	client := NewOasisDBClient("http://localhost:8080")

	// 1. Health check
	ok, err := client.HealthCheck()
	if err != nil {
		panic(err)
	}
	fmt.Println("Health check:", ok)

	// 2. Create collection
	_, err = client.CreateCollection("demo", 128, "hnsw", nil)
	if err != nil {
		panic(err)
	}
	fmt.Println("Created collection: demo")

	// 3. Upsert documents
	docs := make([]map[string]any, 10)
	for i := 0; i < 10; i++ {
		docs[i] = map[string]any{
			"id":     fmt.Sprintf("%d", i),
			"vector": randomVector(128),
		}
	}
	err = client.BatchUpsertDocuments("demo", docs)
	if err != nil {
		panic(err)
	}
	fmt.Println("Upserted 10 documents")

	// 4a. Vector search
	queryVec := randomVector(128)
	res, err := client.SearchVectors("demo", queryVec, 3)
	if err != nil {
		panic(err)
	}
	fmt.Println("Vector search results:", res)

	// 4b. Document search
	res, err = client.SearchDocuments("demo", queryVec, 3, nil)
	if err != nil {
		panic(err)
	}
	fmt.Println("Document search results:", res)

	// 5. Clean up
	err = client.DeleteCollection("demo")
	if err != nil {
		panic(err)
	}
	fmt.Println("Deleted collection 'demo'")
}
