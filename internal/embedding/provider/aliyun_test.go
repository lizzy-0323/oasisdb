package provider

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// mockDashScopeServer creates a test server that mimics DashScope embedding API.
func mockDashScopeServer() *httptest.Server {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req AliyunEmbeddingRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// Determine number of inputs
		var inputCount int
		switch v := req.Input.(type) {
		case string:
			inputCount = 1
		case []interface{}:
			inputCount = len(v)
		case []string:
			inputCount = len(v)
		default:
			inputCount = 1
		}

		resp := AliyunEmbeddingResponse{}
		for i := 0; i < inputCount; i++ {
			resp.Data = append(resp.Data, struct {
				Embedding []float64 `json:"embedding"`
			}{Embedding: []float64{float64(i), float64(i + 1), float64(i + 2)}})
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})

	return httptest.NewServer(handler)
}

func TestAliyunEmbeddingProvider_Embed(t *testing.T) {
	// change the following line to use your own api key and use the real URL
	server := mockDashScopeServer()
	defer server.Close()

	os.Setenv("DASHSCOPE_API_KEY", "YOUR_API_KEY")
	os.Setenv("DASHSCOPE_API_URL", server.URL)

	provider, err := NewAliyunEmbeddingProvider()
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}
	// Override API URL to our mock server
	provider.(*AliyunEmbeddingProvider).apiURL = os.Getenv("DASHSCOPE_API_URL")

	vec, err := provider.Embed("hello")
	if err != nil {
		t.Fatalf("Embed returned error: %v", err)
	}
	assert.NotNil(t, vec)
}

func TestAliyunEmbeddingProvider_EmbedBatch(t *testing.T) {
	// change the following line to use your own api key and use the real URL
	server := mockDashScopeServer()
	defer server.Close()

	os.Setenv("DASHSCOPE_API_KEY", "YOUR_API_KEY")
	os.Setenv("DASHSCOPE_API_URL", server.URL)

	provider, _ := NewAliyunEmbeddingProvider()
	provider.(*AliyunEmbeddingProvider).apiURL = os.Getenv("DASHSCOPE_API_URL")

	inputs := []string{"foo", "bar"}
	vecs, err := provider.EmbedBatch(inputs)
	if err != nil {
		t.Fatalf("EmbedBatch returned error: %v", err)
	}
	assert.Equal(t, len(vecs), len(inputs))
	for _, v := range vecs {
		assert.NotNil(t, v)
	}
}
