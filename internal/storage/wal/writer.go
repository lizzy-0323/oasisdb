package wal

import (
	"encoding/binary"
	"os"
	"path/filepath"
)

type WALWriter struct {
	file      string
	dest      *os.File
	assistBuf [30]byte
}

func NewWALWriter(file string) (*WALWriter, error) {
	// 确保目录存在
	dir := filepath.Dir(file)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	dest, err := os.OpenFile(file, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	return &WALWriter{
		file: file,
		dest: dest,
	}, nil
}

func (w *WALWriter) Write(key, value []byte) error {
	n := binary.PutUvarint(w.assistBuf[0:], uint64(len(key)))
	n += binary.PutUvarint(w.assistBuf[n:], uint64(len(value)))

	var buf []byte
	buf = append(buf, w.assistBuf[:n]...)
	buf = append(buf, key...)
	buf = append(buf, value...)
	_, err := w.dest.Write(buf)
	return err
}

func (w *WALWriter) Close() {
	_ = w.dest.Close()
}
