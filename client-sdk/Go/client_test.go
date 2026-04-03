package main

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"reflect"
	"strings"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func normalizeJSON(t *testing.T, v any) any {
	t.Helper()
	if v == nil {
		return nil
	}

	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("failed to marshal value: %v", err)
	}

	var normalized any
	if err := json.Unmarshal(raw, &normalized); err != nil {
		t.Fatalf("failed to unmarshal value: %v", err)
	}

	return normalized
}

func TestClientMethods(t *testing.T) {
	tests := []struct {
		name         string
		responseBody string
		responseCode int
		wantMethod   string
		wantPath     string
		wantBody     any
		run          func(*OasisDBClient) (any, error)
		assertResult func(*testing.T, any)
	}{
		{
			name:         "HealthCheck",
			responseBody: `{"status":"ok"}`,
			responseCode: http.StatusOK,
			wantMethod:   http.MethodGet,
			wantPath:     "/",
			run: func(c *OasisDBClient) (any, error) {
				return c.HealthCheck()
			},
			assertResult: func(t *testing.T, result any) {
				t.Helper()
				healthy, ok := result.(bool)
				if !ok {
					t.Fatalf("expected bool result, got %T", result)
				}
				if !healthy {
					t.Fatal("expected health check to report healthy")
				}
			},
		},
		{
			name:         "CreateCollection",
			responseBody: `{"name":"docs","dimension":3}`,
			responseCode: http.StatusOK,
			wantMethod:   http.MethodPost,
			wantPath:     "/v1/collections",
			wantBody: map[string]any{
				"name":       "docs",
				"dimension":  3,
				"index_type": "hnsw",
				"parameters": map[string]any{"ef": 64},
			},
			run: func(c *OasisDBClient) (any, error) {
				return c.CreateCollection("docs", 3, "hnsw", map[string]any{"ef": 64})
			},
			assertResult: func(t *testing.T, result any) {
				t.Helper()
				got := result.(map[string]any)
				if got["name"] != "docs" {
					t.Fatalf("expected collection name docs, got %v", got["name"])
				}
			},
		},
		{
			name:         "GetCollection",
			responseBody: `{"name":"docs","dimension":3}`,
			responseCode: http.StatusOK,
			wantMethod:   http.MethodGet,
			wantPath:     "/v1/collections/docs",
			run: func(c *OasisDBClient) (any, error) {
				return c.GetCollection("docs")
			},
			assertResult: func(t *testing.T, result any) {
				t.Helper()
				got := result.(map[string]any)
				if got["name"] != "docs" {
					t.Fatalf("expected collection name docs, got %v", got["name"])
				}
			},
		},
		{
			name:         "ListCollections",
			responseBody: `[{"name":"docs"},{"name":"logs"}]`,
			responseCode: http.StatusOK,
			wantMethod:   http.MethodGet,
			wantPath:     "/v1/collections",
			run: func(c *OasisDBClient) (any, error) {
				return c.ListCollections()
			},
			assertResult: func(t *testing.T, result any) {
				t.Helper()
				got := result.([]map[string]any)
				if len(got) != 2 {
					t.Fatalf("expected 2 collections, got %d", len(got))
				}
			},
		},
		{
			name:         "DeleteCollection",
			responseBody: `{}`,
			responseCode: http.StatusOK,
			wantMethod:   http.MethodDelete,
			wantPath:     "/v1/collections/docs",
			run: func(c *OasisDBClient) (any, error) {
				return nil, c.DeleteCollection("docs")
			},
		},
		{
			name:         "UpsertDocument",
			responseBody: `{"id":"doc-1"}`,
			responseCode: http.StatusOK,
			wantMethod:   http.MethodPost,
			wantPath:     "/v1/collections/docs/documents",
			wantBody: map[string]any{
				"id":         "doc-1",
				"vector":     []float32{1, 2, 3},
				"parameters": map[string]any{"tag": "news"},
			},
			run: func(c *OasisDBClient) (any, error) {
				return c.UpsertDocument("docs", "doc-1", []float32{1, 2, 3}, map[string]any{"tag": "news"})
			},
			assertResult: func(t *testing.T, result any) {
				t.Helper()
				got := result.(map[string]any)
				if got["id"] != "doc-1" {
					t.Fatalf("expected document id doc-1, got %v", got["id"])
				}
			},
		},
		{
			name:         "BatchUpsertDocuments",
			responseBody: `{}`,
			responseCode: http.StatusOK,
			wantMethod:   http.MethodPost,
			wantPath:     "/v1/collections/docs/documents/batchupsert",
			wantBody: map[string]any{
				"documents": []map[string]any{
					{"id": "doc-1"},
					{"id": "doc-2"},
				},
			},
			run: func(c *OasisDBClient) (any, error) {
				return nil, c.BatchUpsertDocuments("docs", []map[string]any{
					{"id": "doc-1"},
					{"id": "doc-2"},
				})
			},
		},
		{
			name:         "GetDocument",
			responseBody: `{"id":"doc-1"}`,
			responseCode: http.StatusOK,
			wantMethod:   http.MethodGet,
			wantPath:     "/v1/collections/docs/documents/doc-1",
			run: func(c *OasisDBClient) (any, error) {
				return c.GetDocument("docs", "doc-1")
			},
			assertResult: func(t *testing.T, result any) {
				t.Helper()
				got := result.(map[string]any)
				if got["id"] != "doc-1" {
					t.Fatalf("expected document id doc-1, got %v", got["id"])
				}
			},
		},
		{
			name:         "DeleteDocument",
			responseBody: `{}`,
			responseCode: http.StatusOK,
			wantMethod:   http.MethodDelete,
			wantPath:     "/v1/collections/docs/documents/doc-1",
			run: func(c *OasisDBClient) (any, error) {
				return nil, c.DeleteDocument("docs", "doc-1")
			},
		},
		{
			name:         "BuildIndex",
			responseBody: `{}`,
			responseCode: http.StatusOK,
			wantMethod:   http.MethodPost,
			wantPath:     "/v1/collections/docs/buildindex",
			wantBody: map[string]any{
				"documents": []map[string]any{
					{"id": "doc-1"},
				},
			},
			run: func(c *OasisDBClient) (any, error) {
				return nil, c.BuildIndex("docs", []map[string]any{{"id": "doc-1"}})
			},
		},
		{
			name:         "SetParams",
			responseBody: `{}`,
			responseCode: http.StatusOK,
			wantMethod:   http.MethodPost,
			wantPath:     "/v1/collections/docs/documents/setparams",
			wantBody: map[string]any{
				"parameters": map[string]any{"ef_search": 128},
			},
			run: func(c *OasisDBClient) (any, error) {
				return nil, c.SetParams("docs", map[string]any{"ef_search": 128})
			},
		},
		{
			name:         "SearchVectors",
			responseBody: `{"results":[{"id":"doc-1"}]}`,
			responseCode: http.StatusOK,
			wantMethod:   http.MethodPost,
			wantPath:     "/v1/collections/docs/vectors/search",
			wantBody: map[string]any{
				"vector": []float32{1, 2, 3},
				"limit":  2,
			},
			run: func(c *OasisDBClient) (any, error) {
				return c.SearchVectors("docs", []float32{1, 2, 3}, 2)
			},
			assertResult: func(t *testing.T, result any) {
				t.Helper()
				got := result.(map[string]any)
				results, ok := got["results"].([]any)
				if !ok || len(results) != 1 {
					t.Fatalf("expected one result, got %v", got["results"])
				}
			},
		},
		{
			name:         "SearchDocumentsWithoutFilter",
			responseBody: `{"results":[{"id":"doc-1"}]}`,
			responseCode: http.StatusOK,
			wantMethod:   http.MethodPost,
			wantPath:     "/v1/collections/docs/documents/search",
			wantBody: map[string]any{
				"vector": []float32{1, 2, 3},
				"limit":  2,
			},
			run: func(c *OasisDBClient) (any, error) {
				return c.SearchDocuments("docs", []float32{1, 2, 3}, 2, nil)
			},
		},
		{
			name:         "SearchDocumentsWithFilter",
			responseBody: `{"results":[{"id":"doc-1"}]}`,
			responseCode: http.StatusOK,
			wantMethod:   http.MethodPost,
			wantPath:     "/v1/collections/docs/documents/search",
			wantBody: map[string]any{
				"vector": []float32{1, 2, 3},
				"limit":  2,
				"filter": map[string]any{"tag": "news"},
			},
			run: func(c *OasisDBClient) (any, error) {
				return c.SearchDocuments("docs", []float32{1, 2, 3}, 2, map[string]any{"tag": "news"})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotMethod string
			var gotPath string
			var gotContentType string
			var gotBody any

			client := NewOasisDBClient("http://example.com")
			client.Client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				gotMethod = r.Method
				gotPath = r.URL.Path
				gotContentType = r.Header.Get("Content-Type")

				if r.Body != nil {
					body, err := io.ReadAll(r.Body)
					if err != nil {
						t.Fatalf("failed to read request body: %v", err)
					}
					if len(body) > 0 {
						if err := json.Unmarshal(body, &gotBody); err != nil {
							t.Fatalf("failed to decode request body: %v", err)
						}
					}
				}

				return &http.Response{
					StatusCode: tt.responseCode,
					Body:       io.NopCloser(strings.NewReader(tt.responseBody)),
					Header:     make(http.Header),
				}, nil
			})}
			result, err := tt.run(client)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if gotMethod != tt.wantMethod {
				t.Fatalf("expected method %s, got %s", tt.wantMethod, gotMethod)
			}
			if gotPath != tt.wantPath {
				t.Fatalf("expected path %s, got %s", tt.wantPath, gotPath)
			}
			if gotContentType != "application/json" {
				t.Fatalf("expected content type application/json, got %s", gotContentType)
			}

			if tt.wantBody == nil {
				if gotBody != nil {
					t.Fatalf("expected empty request body, got %v", gotBody)
				}
			} else if !reflect.DeepEqual(normalizeJSON(t, tt.wantBody), gotBody) {
				t.Fatalf("unexpected request body: want %v, got %v", normalizeJSON(t, tt.wantBody), gotBody)
			}

			if tt.assertResult != nil {
				tt.assertResult(t, result)
			}
		})
	}
}

func TestRequestReturnsOasisDBError(t *testing.T) {
	client := NewOasisDBClient("http://example.com")
	client.Client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusConflict,
			Body:       io.NopCloser(strings.NewReader("collection already exists\n")),
			Header:     make(http.Header),
		}, nil
	})}
	_, err := client.request(http.MethodGet, "/v1/collections/docs", nil)
	if err == nil {
		t.Fatal("expected request to fail")
	}

	var oasisErr *OasisDBError
	if !errors.As(err, &oasisErr) {
		t.Fatalf("expected OasisDBError, got %T", err)
	}
	if oasisErr.StatusCode != http.StatusConflict {
		t.Fatalf("expected status code %d, got %d", http.StatusConflict, oasisErr.StatusCode)
	}
	if !strings.Contains(oasisErr.Error(), "collection already exists") {
		t.Fatalf("unexpected error string: %s", oasisErr.Error())
	}
}

func TestRequestReturnsMarshalError(t *testing.T) {
	client := NewOasisDBClient("http://example.com")
	_, err := client.request(http.MethodPost, "/broken", map[string]any{
		"invalid": func() {},
	})
	if err == nil {
		t.Fatal("expected json marshal to fail")
	}
}

func TestHealthCheckReturnsFalseForNonOKStatus(t *testing.T) {
	client := NewOasisDBClient("http://example.com")
	client.Client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"status":"down"}`)),
			Header:     make(http.Header),
		}, nil
	})}
	healthy, err := client.HealthCheck()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if healthy {
		t.Fatal("expected health check to report unhealthy")
	}
}

func TestGetCollectionReturnsJSONError(t *testing.T) {
	client := NewOasisDBClient("http://example.com")
	client.Client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{not-json`)),
			Header:     make(http.Header),
		}, nil
	})}
	_, err := client.GetCollection("docs")
	if err == nil {
		t.Fatal("expected invalid json response to fail")
	}
}

func TestExampleProgramMain(t *testing.T) {
	oldTransport := http.DefaultTransport
	oldStdout := os.Stdout
	defer func() {
		http.DefaultTransport = oldTransport
		os.Stdout = oldStdout
	}()

	var calls []string
	http.DefaultTransport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls = append(calls, r.Method+" "+r.URL.Path)

		if r.URL.Path == "/v1/collections/demo/documents/batchupsert" {
			var payload map[string]any
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Fatalf("failed to decode batch upsert payload: %v", err)
			}
			documents, ok := payload["documents"].([]any)
			if !ok || len(documents) != 10 {
				t.Fatalf("expected 10 documents in batch upsert, got %v", payload["documents"])
			}
		}

		var body string
		switch r.Method + " " + r.URL.Path {
		case "GET /":
			body = `{"status":"ok"}`
		case "POST /v1/collections":
			body = `{"name":"demo"}`
		case "POST /v1/collections/demo/documents/batchupsert":
			body = `{}`
		case "POST /v1/collections/demo/vectors/search":
			body = `{"results":[]}`
		case "POST /v1/collections/demo/documents/search":
			body = `{"results":[]}`
		case "DELETE /v1/collections/demo":
			body = `{}`
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}

		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(body)),
			Header:     make(http.Header),
		}, nil
	})

	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("failed to create stdout pipe: %v", err)
	}
	os.Stdout = writer

	main()

	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close stdout writer: %v", err)
	}
	os.Stdout = oldStdout

	output, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("failed to read stdout: %v", err)
	}

	expectedCalls := []string{
		"GET /",
		"POST /v1/collections",
		"POST /v1/collections/demo/documents/batchupsert",
		"POST /v1/collections/demo/vectors/search",
		"POST /v1/collections/demo/documents/search",
		"DELETE /v1/collections/demo",
	}
	if !reflect.DeepEqual(expectedCalls, calls) {
		t.Fatalf("unexpected request sequence: want %v, got %v", expectedCalls, calls)
	}

	outputText := string(output)
	expectedOutput := []string{
		"Health check: true",
		"Created collection: demo",
		"Upserted 10 documents",
		"Vector search results:",
		"Document search results:",
		"Deleted collection 'demo'",
	}
	for _, want := range expectedOutput {
		if !strings.Contains(outputText, want) {
			t.Fatalf("expected output to contain %q, got %q", want, outputText)
		}
	}
}
