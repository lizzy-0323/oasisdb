package memtable

import (
	"bytes"
	"math/rand"
	"sync"
)

const (
	MAX_LEVEL = 16
	P         = 0.25
)

type SkipList struct {
	mu         *sync.RWMutex
	head       *skipListNode
	size       int // data size
	entriesCnt int // num of entries
	level      int // max level for skiplist
}

type skipListNode struct {
	key, value []byte
	next       []*skipListNode
}

func NewSkipList() *SkipList {
	return &SkipList{
		head: &skipListNode{
			next: make([]*skipListNode, 1), // Initialize with level 1
		},
		level:      1,
		entriesCnt: 0,
		size:       0,
		mu:         &sync.RWMutex{},
	}
}

// This function is thread-safe, instead of using s.size
func (s *SkipList) Size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.size
}

func (s *SkipList) EntriesCnt() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.entriesCnt
}

func (s *SkipList) Put(key, value []byte) error {
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

	newNode := &skipListNode{
		key:   key,
		value: value,
		next:  make([]*skipListNode, newLevel),
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

func (s *SkipList) GetRandomLevel() int {
	level := 1
	for rand.Float32() < P && level < MAX_LEVEL {
		level++
	}
	return level
}

// searchInternal performs the search operation without locking
func (s *SkipList) searchInternal(key []byte) *skipListNode {
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
func (s *SkipList) Search(key []byte) *skipListNode {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.searchInternal(key)
}

func (s *SkipList) Get(key []byte) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if node := s.searchInternal(key); node != nil {
		return node.value, nil
	}
	return nil, nil
}

func (s *SkipList) All() []*KVPair {
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
