package wal

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestWALReaderReadAllRejectsMalformedVarint(t *testing.T) {
	reader := &WALReader{}
	_, err := reader.readAll(bytes.NewReader(bytes.Repeat([]byte{0x80}, 11)))
	if err == nil {
		t.Fatal("expected malformed varint to fail")
	}
}

func TestWALReaderReadAllRejectsMissingValueLength(t *testing.T) {
	reader := &WALReader{}
	_, err := reader.readAll(bytes.NewReader([]byte{1}))
	if err == nil {
		t.Fatal("expected truncated value length to fail")
	}
}

func TestWALReaderReadAllRejectsMissingKeyBytes(t *testing.T) {
	reader := &WALReader{}
	_, err := reader.readAll(bytes.NewReader([]byte{1, 1}))
	if err == nil {
		t.Fatal("expected truncated key bytes to fail")
	}
}

func TestWALReaderReadAllRejectsMissingValueBytes(t *testing.T) {
	reader := &WALReader{}
	_, err := reader.readAll(bytes.NewReader([]byte{1, 1, 'k'}))
	if err == nil {
		t.Fatal("expected truncated value bytes to fail")
	}
}

func TestWALReaderRestoreToMemtableReturnsDecodeError(t *testing.T) {
	tmpDir := t.TempDir()
	walFile := filepath.Join(tmpDir, "broken.wal")
	if err := os.WriteFile(walFile, []byte{1}, 0644); err != nil {
		t.Fatalf("failed to write broken wal file: %v", err)
	}

	reader, err := NewWALReader(walFile)
	if err != nil {
		t.Fatalf("failed to create reader: %v", err)
	}
	defer reader.Close()

	err = reader.RestoreToMemtable(NewMockMemTable())
	if err == nil {
		t.Fatal("expected restore to fail for broken wal content")
	}
}

func TestWALWriterNewWALWriterReturnsOpenFileError(t *testing.T) {
	tmpDir := t.TempDir()
	dirAsFile := filepath.Join(tmpDir, "wal-dir")
	if err := os.Mkdir(dirAsFile, 0755); err != nil {
		t.Fatalf("failed to create directory: %v", err)
	}

	writer, err := NewWALWriter(dirAsFile)
	if err == nil {
		if writer != nil {
			writer.Close()
		}
		t.Fatal("expected opening a directory as a wal file to fail")
	}
	if writer != nil {
		t.Fatal("expected writer to be nil when opening a directory fails")
	}
}

func TestWALWriterWriteAfterCloseReturnsError(t *testing.T) {
	tmpDir := t.TempDir()
	walFile := filepath.Join(tmpDir, "closed.wal")

	writer, err := NewWALWriter(walFile)
	if err != nil {
		t.Fatalf("failed to create wal writer: %v", err)
	}
	writer.Close()

	if err := writer.Write([]byte("key"), []byte("value")); err == nil {
		t.Fatal("expected writing to a closed wal writer to fail")
	}
}
