package wal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"oasisdb/internal/storage/memtable"
	"os"
)

type WALReader struct {
	file   string
	src    *os.File
	reader *bufio.Reader
}

func NewWALReader(file string) (*WALReader, error) {
	src, err := os.OpenFile(file, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	return &WALReader{
		file:   file,
		src:    src,
		reader: bufio.NewReader(src),
	}, nil
}

func (w *WALReader) RestoreToMemtable(memTable memtable.MemTable) error {
	// read all content
	body, err := io.ReadAll(w.reader)
	if err != nil {
		return err
	}

	// reset file offset to start
	defer func() {
		_, _ = w.src.Seek(0, io.SeekStart)
	}()

	// parse content
	kvs, err := w.readAll(bytes.NewReader(body))
	if err != nil {
		return err
	}

	// inject all kv data to memtable
	for _, kv := range kvs {
		memTable.Put(kv.Key, kv.Value)
	}

	return nil
}

func (w *WALReader) readAll(reader *bytes.Reader) ([]*memtable.KVPair, error) {
	var kvs []*memtable.KVPair
	for {
		// read key length
		keyLen, err := binary.ReadUvarint(reader)
		// if encounter eof error, break
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}

		// read value length
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}

		// read value length
		valLen, err := binary.ReadUvarint(reader)
		if err != nil {
			return nil, err
		}

		// read key
		keyBuf := make([]byte, keyLen)
		if _, err = io.ReadFull(reader, keyBuf); err != nil {
			return nil, err
		}

		// read value
		valBuf := make([]byte, valLen)
		if _, err = io.ReadFull(reader, valBuf); err != nil {
			return nil, err
		}

		kvs = append(kvs, &memtable.KVPair{
			Key:   keyBuf,
			Value: valBuf,
		})
	}

	return kvs, nil
}

func (w *WALReader) Close() {
	w.reader.Reset(w.src)
	_ = w.src.Close()
}
