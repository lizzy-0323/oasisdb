package storage

type Storage struct {
}

func (s *Storage) Put(key []byte, value []byte) error {
	return nil
}

func (s *Storage) Get(key []byte) ([]byte, bool, error) {
	return nil, false, nil
}
