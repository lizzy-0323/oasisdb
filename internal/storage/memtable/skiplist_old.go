package memtable

import (
	"bytes"
	"math/rand"
	"sync"
)

type SkipListOld struct {
	mu         sync.RWMutex
	head       *skipListNodeOld
	size       int // data size
	entriesCnt int // num of entries
	level      int // max level for skiplist
}

type skipListNodeOld struct {
	key, value []byte
	next       []*skipListNodeOld
}

func NewSkipListOld() MemTable {
	return &SkipListOld{
		head: &skipListNodeOld{
			next: make([]*skipListNodeOld, 1), // Initialize with level 1
		},
		level:      1,
		entriesCnt: 0,
		size:       0,
		mu:         sync.RWMutex{},
	}
}

// This function is thread-safe, instead of using s.size
func (s *SkipListOld) Size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.size
}

func (s *SkipListOld) EntriesCnt() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.entriesCnt
}

func (s *SkipListOld) Put(key, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// if key exists
	if node := s.searchInternal(key); node != nil {
		s.size += len(value) - len(node.value)
		node.value = value
		return nil
	}
	// if key not exist, insert it
	s.entriesCnt++
	s.size += len(key) + len(value)

	newLevel := s.GetRandomLevel()
	if s.level < newLevel {
		// if level is greater than current level, add new level
		for i := s.level + 1; i <= newLevel; i++ {
			s.head.next = append(s.head.next, nil)
		}
		s.level = newLevel
	}

	newNode := &skipListNodeOld{
		key:   key,
		value: value,
		next:  make([]*skipListNodeOld, newLevel),
	}

	// insert node from top to bottom
	cur := s.head
	for i := newLevel - 1; i >= 0; i-- {
		for cur.next[i] != nil && bytes.Compare(cur.next[i].key, key) < 0 {
			cur = cur.next[i]
		}
		// Insert newNode
		newNode.next[i] = cur.next[i]
		cur.next[i] = newNode
	}
	return nil
}

func (s *SkipListOld) GetRandomLevel() int {
	level := 1
	for rand.Float32() < P && level < MAX_LEVEL {
		level++
	}
	return level
}

// searchInternal performs the search operation without locking
func (s *SkipListOld) searchInternal(key []byte) *skipListNodeOld {
	cur := s.head
	for i := s.level - 1; i >= 0; i-- {
		for cur.next[i] != nil && bytes.Compare(cur.next[i].key, key) < 0 {
			cur = cur.next[i]
		}
		if cur.next[i] != nil && bytes.Equal(cur.next[i].key, key) {
			return cur.next[i]
		}
	}
	return nil
}

// Search returns the node with the given key, thread-safe
func (s *SkipListOld) Search(key []byte) *skipListNodeOld {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.searchInternal(key)
}

func (s *SkipListOld) Get(key []byte) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if node := s.searchInternal(key); node != nil {
		return node.value, true
	}
	return nil, false
}

func (s *SkipListOld) All() []*KVPair {
	if s.entriesCnt == 0 {
		return nil
	}
	nodes := make([]*KVPair, 0, s.entriesCnt)
	cur := s.head
	for i := s.level - 1; i >= 0; i-- {
		for cur.next[i] != nil {
			cur = cur.next[i]
			nodes = append(nodes, &KVPair{
				Key:   cur.key,
				Value: cur.value,
			})
		}
	}
	return nodes
}
