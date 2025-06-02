package filter

import (
	"testing"
)

func TestNewBloomFilter(t *testing.T) {
	tests := []struct {
		name     string
		m        int
		expected int
	}{
		{"default size", 0, DefaultBloomFilterM},
		{"custom size", 2048, 2048},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bf := NewBloomFilter(tt.m)
			if bf.m != tt.expected {
				t.Errorf("NewBloomFilter(%d) got m = %d, want %d", tt.m, bf.m, tt.expected)
			}
		})
	}
}

func TestBloomFilter_Add_MayContain(t *testing.T) {
	bf := NewBloomFilter(1024)

	// Test adding and checking for existence
	testKeys := [][]byte{
		[]byte("key1"),
		[]byte("key2"),
		[]byte("key3"),
	}

	// Add keys
	for _, key := range testKeys {
		bf.Add(key)
	}

	// Test for added keys
	bitmap := bf.Hash()
	for _, key := range testKeys {
		if !bf.MayContain(bitmap, key) {
			t.Errorf("MayContain(%s) = false, want true", string(key))
		}
	}

	// Test for non-existent key (may have false positives)
	nonExistentKey := []byte("nonexistent")
	// Note: We can't assert it must return false due to bloom filter's nature
	// but we can note if it's a false positive
	if bf.MayContain(bitmap, nonExistentKey) {
		t.Logf("Note: False positive detected for key: %s", string(nonExistentKey))
	}
}

func TestBloomFilter_GetBestK(t *testing.T) {
	bf := NewBloomFilter(1024)

	// Add some keys
	for i := 0; i < 100; i++ {
		bf.Add([]byte{byte(i)})
	}

	k := bf.GetBestK()
	if k < 1 || k > 30 {
		t.Errorf("GetBestK() = %d, want value between 1 and 30", k)
	}
}

func TestBloomFilter_Reset(t *testing.T) {
	bf := NewBloomFilter(1024)

	// Add some keys
	testKeys := [][]byte{
		[]byte("key1"),
		[]byte("key2"),
	}

	for _, key := range testKeys {
		bf.Add(key)
	}

	if bf.KeyLen() != 2 {
		t.Errorf("KeyLen() before reset = %d, want 2", bf.KeyLen())
	}

	bf.Reset()

	if bf.KeyLen() != 0 {
		t.Errorf("KeyLen() after reset = %d, want 0", bf.KeyLen())
	}

	// Verify that the filter no longer contains previously added keys
	bitmap := bf.Hash()
	for _, key := range testKeys {
		if bf.MayContain(bitmap, key) {
			t.Errorf("After reset, key %s should not be present", string(key))
		}
	}
}
