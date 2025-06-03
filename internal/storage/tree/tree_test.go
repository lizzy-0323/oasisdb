package tree

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"oasisdb/internal/config"
)

func setupTestLSMTree(t *testing.T) (*LSMTree, string) {
	// Create temporary directory for test
	tmpDir, err := os.MkdirTemp("", "lsmtree_test_*")
	if err != nil {
		t.Fatal(err)
	}

	// Create WAL directory
	if err := os.MkdirAll(path.Join(tmpDir, "walfile", "memtable"), 0755); err != nil {
		t.Fatal(err)
	}

	// Create SST directory in conf.Dir
	if err := os.MkdirAll(path.Join(tmpDir, "sstfile"), 0755); err != nil {
		t.Fatal(err)
	}

	// Create config
	conf, err := config.NewConfig(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	// Create LSM tree
	lsm, err := NewLSMTree(conf)
	if err != nil {
		t.Fatal(err)
	}

	return lsm, tmpDir
}

func cleanupTestLSMTree(t *testing.T, lsm *LSMTree, dir string) {
	lsm.Stop()
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
}

func TestLSMTreeBasicOperations(t *testing.T) {
	lsm, tmpDir := setupTestLSMTree(t)
	defer cleanupTestLSMTree(t, lsm, tmpDir)

	// Test Put
	key := []byte("test_key")
	value := []byte("test_value")
	if err := lsm.Put(key, value); err != nil {
		t.Errorf("Put failed: %v", err)
	}

	// Test Get
	result, exists, err := lsm.Get(key)
	if err != nil {
		t.Errorf("Get failed: %v", err)
	}
	if !exists {
		t.Error("Key should exist")
	}
	if !bytes.Equal(result, value) {
		t.Errorf("Expected value %s, got %s", value, result)
	}

	// Test non-existent key
	nonExistentKey := []byte("non_existent")
	_, exists, err = lsm.Get(nonExistentKey)
	if err != nil {
		t.Errorf("Get failed: %v", err)
	}
	if exists {
		t.Error("Key should not exist")
	}
}

func TestLSMTreeMultipleOperations(t *testing.T) {
	lsm, tmpDir := setupTestLSMTree(t)
	defer cleanupTestLSMTree(t, lsm, tmpDir)

	// Insert multiple key-value pairs
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("value_%d", i))
		if err := lsm.Put(key, value); err != nil {
			t.Errorf("Put failed for key %s: %v", key, err)
		}
	}

	// Verify all insertions
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		expectedValue := []byte(fmt.Sprintf("value_%d", i))

		result, exists, err := lsm.Get(key)
		if err != nil {
			t.Errorf("Get failed for key %s: %v", key, err)
		}
		if !exists {
			t.Errorf("Key %s should exist", key)
		}
		if !bytes.Equal(result, expectedValue) {
			t.Errorf("For key %s: expected value %s, got %s", key, expectedValue, result)
		}
	}
}

func TestLSMTreeCompaction(t *testing.T) {
	lsm, tmpDir := setupTestLSMTree(t)
	defer cleanupTestLSMTree(t, lsm, tmpDir)

	// Insert enough data to trigger compaction
	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("compaction_key_%d", i))
		value := []byte(fmt.Sprintf("compaction_value_%d", i))
		if err := lsm.Put(key, value); err != nil {
			t.Errorf("Put failed for key %s: %v", key, err)
		}
	}

	// Give some time for compaction to happen
	time.Sleep(time.Second)

	// Verify data after compaction
	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("compaction_key_%d", i))
		expectedValue := []byte(fmt.Sprintf("compaction_value_%d", i))

		result, exists, err := lsm.Get(key)
		if err != nil {
			t.Errorf("Get failed for key %s: %v", key, err)
		}
		if !exists {
			t.Errorf("Key %s should exist", key)
		}
		if !bytes.Equal(result, expectedValue) {
			t.Errorf("For key %s: expected value %s, got %s", key, expectedValue, result)
		}
	}
}
