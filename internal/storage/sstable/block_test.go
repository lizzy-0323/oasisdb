package sstable

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func Test_Block_ToBytes(t *testing.T) {
	block := NewBlock()

	// Add some test data
	block.Append([]byte("key1"), []byte("value1"))
	block.Append([]byte("key2"), []byte("value2"))
	block.Append([]byte("longerkey3"), []byte("longervalue3"))

	// Create expected output
	expect := bytes.NewBuffer([]byte{})

	// First entry: "key1" -> "value1"
	binary.Write(expect, binary.LittleEndian, uint16(4)) // key length
	binary.Write(expect, binary.LittleEndian, uint32(6)) // value length
	expect.Write([]byte("key1"))
	expect.Write([]byte("value1"))

	// Second entry: "key2" -> "value2"
	binary.Write(expect, binary.LittleEndian, uint16(4)) // key length
	binary.Write(expect, binary.LittleEndian, uint32(6)) // value length
	expect.Write([]byte("key2"))
	expect.Write([]byte("value2"))

	// Third entry: "longerkey3" -> "longervalue3"
	binary.Write(expect, binary.LittleEndian, uint16(10)) // key length
	binary.Write(expect, binary.LittleEndian, uint32(12)) // value length
	expect.Write([]byte("longerkey3"))
	expect.Write([]byte("longervalue3"))

	// Compare with actual output
	if !bytes.Equal(block.record.Bytes(), expect.Bytes()) {
		t.Errorf("Block bytes do not match expected output.\nGot: %v\nWant: %v", block.record.Bytes(), expect.Bytes())
	}
}
