package index

import (
	"math"
	"testing"
)

func TestDistance(t *testing.T) {
	tests := []struct {
		name     string
		a        []float32
		b        []float32
		space    SpaceType
		expected float32
		epsilon  float32
	}{
		// L2 Space tests
		{
			name:     "L2 identical vectors",
			a:        []float32{1, 2, 3},
			b:        []float32{1, 2, 3},
			space:    L2Space,
			expected: 0,
			epsilon:  1e-6,
		},
		{
			name:     "L2 different vectors",
			a:        []float32{1, 2, 3},
			b:        []float32{4, 5, 6},
			space:    L2Space,
			expected: 27, // (1-4)^2 + (2-5)^2 + (3-6)^2 = 9 + 9 + 9 = 27
			epsilon:  1e-6,
		},
		{
			name:     "L2 zero vectors",
			a:        []float32{0, 0, 0},
			b:        []float32{0, 0, 0},
			space:    L2Space,
			expected: 0,
			epsilon:  1e-6,
		},

		// Inner Product Space tests
		{
			name:     "IP identical vectors",
			a:        []float32{1, 2, 3},
			b:        []float32{1, 2, 3},
			space:    IPSpace,
			expected: -14, // -(1*1 + 2*2 + 3*3) = -14
			epsilon:  1e-6,
		},
		{
			name:     "IP different vectors",
			a:        []float32{1, 2, 3},
			b:        []float32{4, 5, 6},
			space:    IPSpace,
			expected: -32, // -(1*4 + 2*5 + 3*6) = -32
			epsilon:  1e-6,
		},
		{
			name:     "IP orthogonal vectors",
			a:        []float32{1, 0},
			b:        []float32{0, 1},
			space:    IPSpace,
			expected: 0, // -(1*0 + 0*1) = 0
			epsilon:  1e-6,
		},

		// Cosine Space tests
		{
			name:     "Cosine identical vectors",
			a:        []float32{1, 2, 3},
			b:        []float32{1, 2, 3},
			space:    CosSpace,
			expected: 0, // 1 - 1 = 0 (cosine similarity = 1)
			epsilon:  1e-6,
		},
		{
			name:     "Cosine orthogonal vectors",
			a:        []float32{1, 0},
			b:        []float32{0, 1},
			space:    CosSpace,
			expected: 1, // 1 - 0 = 1 (cosine similarity = 0)
			epsilon:  1e-6,
		},
		{
			name:     "Cosine opposite vectors",
			a:        []float32{1, 1},
			b:        []float32{-1, -1},
			space:    CosSpace,
			expected: 2, // 1 - (-1) = 2 (cosine similarity = -1)
			epsilon:  1e-6,
		},
		{
			name:     "Cosine zero vector a",
			a:        []float32{0, 0},
			b:        []float32{1, 1},
			space:    CosSpace,
			expected: 1, // maximal distance when one vector is zero
			epsilon:  1e-6,
		},
		{
			name:     "Cosine zero vector b",
			a:        []float32{1, 1},
			b:        []float32{0, 0},
			space:    CosSpace,
			expected: 1, // maximal distance when one vector is zero
			epsilon:  1e-6,
		},

		// Hamming Space tests
		{
			name:     "Hamming identical vectors",
			a:        []float32{1, 2, 3},
			b:        []float32{1, 2, 3},
			space:    HammingSpace,
			expected: 0,
			epsilon:  1e-6,
		},
		{
			name:     "Hamming all different",
			a:        []float32{1, 2, 3},
			b:        []float32{4, 5, 6},
			space:    HammingSpace,
			expected: 3,
			epsilon:  1e-6,
		},
		{
			name:     "Hamming partially different",
			a:        []float32{1, 2, 3},
			b:        []float32{1, 5, 3},
			space:    HammingSpace,
			expected: 1,
			epsilon:  1e-6,
		},
		{
			name:     "Hamming binary vectors",
			a:        []float32{0, 1, 0, 1},
			b:        []float32{1, 1, 0, 0},
			space:    HammingSpace,
			expected: 2,
			epsilon:  1e-6,
		},

		// Default (L2) space tests
		{
			name:     "Default space (L2)",
			a:        []float32{1, 2},
			b:        []float32{3, 4},
			space:    "unknown", // Should default to L2
			expected: 8,         // (1-3)^2 + (2-4)^2 = 4 + 4 = 8
			epsilon:  1e-6,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := distance(tt.a, tt.b, tt.space)
			if math.Abs(float64(result-tt.expected)) > float64(tt.epsilon) {
				t.Errorf("distance(%v, %v, %v) = %f, want %f", tt.a, tt.b, tt.space, result, tt.expected)
			}
		})
	}
}

func TestDistanceEdgeCases(t *testing.T) {
	t.Run("empty vectors", func(t *testing.T) {
		a := []float32{}
		b := []float32{}
		
		// All distance functions should handle empty vectors gracefully
		result := distance(a, b, L2Space)
		if result != 0 {
			t.Errorf("L2 distance of empty vectors should be 0, got %f", result)
		}
		
		result = distance(a, b, IPSpace)
		if result != 0 {
			t.Errorf("IP distance of empty vectors should be 0, got %f", result)
		}
		
		result = distance(a, b, CosSpace)
		if result != 1 {
			t.Errorf("Cosine distance of empty vectors should be 1 (maximal), got %f", result)
		}
		
		result = distance(a, b, HammingSpace)
		if result != 0 {
			t.Errorf("Hamming distance of empty vectors should be 0, got %f", result)
		}
	})

	t.Run("single element vectors", func(t *testing.T) {
		a := []float32{5}
		b := []float32{3}
		
		// L2: (5-3)^2 = 4
		result := distance(a, b, L2Space)
		if result != 4 {
			t.Errorf("L2 distance should be 4, got %f", result)
		}
		
		// IP: -(5*3) = -15
		result = distance(a, b, IPSpace)
		if result != -15 {
			t.Errorf("IP distance should be -15, got %f", result)
		}
		
		// Hamming: 5 != 3, so 1
		result = distance(a, b, HammingSpace)
		if result != 1 {
			t.Errorf("Hamming distance should be 1, got %f", result)
		}
	})

	t.Run("large vectors", func(t *testing.T) {
		size := 1000
		a := make([]float32, size)
		b := make([]float32, size)
		
		for i := 0; i < size; i++ {
			a[i] = 1.0
			b[i] = 2.0
		}
		
		// L2: 1000 * (1-2)^2 = 1000
		result := distance(a, b, L2Space)
		if result != 1000 {
			t.Errorf("L2 distance of large vectors should be 1000, got %f", result)
		}
		
		// IP: -(1000 * 1 * 2) = -2000
		result = distance(a, b, IPSpace)
		if result != -2000 {
			t.Errorf("IP distance of large vectors should be -2000, got %f", result)
		}
		
		// Hamming: all 1000 elements are different
		result = distance(a, b, HammingSpace)
		if result != 1000 {
			t.Errorf("Hamming distance of large vectors should be 1000, got %f", result)
		}
	})
}

func BenchmarkDistance(b *testing.B) {
	sizes := []int{10, 100, 1000}
	spaces := []SpaceType{L2Space, IPSpace, CosSpace, HammingSpace}
	
	for _, size := range sizes {
		for _, space := range spaces {
			a := make([]float32, size)
			vec_b := make([]float32, size)
			
			for i := 0; i < size; i++ {
				a[i] = float32(i)
				vec_b[i] = float32(i + 1)
			}
			
			b.Run(string(space)+"_"+string(rune(size+'0')), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					distance(a, vec_b, space)
				}
			})
		}
	}
}