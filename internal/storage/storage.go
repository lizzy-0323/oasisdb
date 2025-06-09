package storage

import (
	"oasisdb/internal/config"
	"oasisdb/internal/storage/tree"
	"oasisdb/pkg/errors"
)

type ScalarStorage interface {
	PutScalar(key []byte, value []byte) error
	BatchPutScalar(keys [][]byte, values [][]byte) error
	GetScalar(key []byte) ([]byte, bool, error)
	DeleteScalar(key []byte) error
	Stop()
}

type Storage struct {
	lsmTree *tree.LSMTree
}

func NewStorage(conf *config.Config) (*Storage, error) {
	lsmTree, err := tree.NewLSMTree(conf)
	if err != nil {
		return nil, err
	}
	return &Storage{lsmTree: lsmTree}, nil
}

func (s *Storage) PutScalar(key []byte, value []byte) error {
	return s.lsmTree.Put(key, value)
}

func (s *Storage) GetScalar(key []byte) ([]byte, bool, error) {
	return s.lsmTree.Get(key)
}

func (s *Storage) DeleteScalar(key []byte) error {
	return s.lsmTree.Put(key, nil)
}

func (s *Storage) BatchPutScalar(keys [][]byte, values [][]byte) error {
	if len(keys) != len(values) {
		return errors.ErrMisMatchKeysAndValues
	}
	for i := range keys {
		if err := s.lsmTree.Put(keys[i], values[i]); err != nil {
			return err
		}
	}
	return nil
}

func (s *Storage) Stop() {
	s.lsmTree.Stop()
}
