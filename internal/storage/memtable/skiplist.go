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
	head       *skipListNode
	size       int
	entriesCnt int
	level      int
	mu         sync.RWMutex // 保护level字段的锁
}

type skipListNode struct {
	mu         sync.RWMutex // 节点级锁
	key, value []byte
	next       []*skipListNode
}

func NewSkipList() MemTable {
	return &SkipList{
		head: &skipListNode{
			next: make([]*skipListNode, MAX_LEVEL),
		},
		level:      1,
		entriesCnt: 0,
		size:       0,
	}
}

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

// findLeftBoundary 找到每一层的左边界节点（需要锁定的节点）
func (s *SkipList) findLeftBoundary(key []byte) ([]*skipListNode, []*skipListNode) {
	s.mu.RLock()
	currentLevel := s.level
	s.mu.RUnlock()

	// 确保不超过最大层数
	if currentLevel > MAX_LEVEL {
		currentLevel = MAX_LEVEL
	}

	update := make([]*skipListNode, MAX_LEVEL) // 左边界节点（需要锁定）
	next := make([]*skipListNode, MAX_LEVEL)   // 下一个节点（用于验证）

	// 初始化所有层级的update为头节点
	for i := 0; i < MAX_LEVEL; i++ {
		update[i] = s.head
	}

	cur := s.head

	// 从最高层向下搜索，找到每层的左边界节点
	for i := currentLevel - 1; i >= 0; i-- {
		// 确保不越界
		if i >= MAX_LEVEL {
			continue
		}

		for cur.next[i] != nil && bytes.Compare(cur.next[i].key, key) < 0 {
			cur = cur.next[i]
		}
		update[i] = cur       // 左边界节点
		next[i] = cur.next[i] // 下一个节点
	}

	return update, next
}

func (s *SkipList) Put(key, value []byte) error {
	for {
		// 第一步：找到左边界节点
		update, next := s.findLeftBoundary(key)

		// 第二步：检查是否已存在相同的key
		if next[0] != nil && bytes.Equal(next[0].key, key) {
			// 更新现有值 - 只需要锁定level 0的左边界节点
			leftBoundary := update[0]
			leftBoundary.mu.Lock()

			// 双重检查：锁定后再次验证
			if leftBoundary.next[0] != nil && bytes.Equal(leftBoundary.next[0].key, key) {
				s.mu.Lock()
				s.size += len(value) - len(leftBoundary.next[0].value)
				s.mu.Unlock()

				leftBoundary.next[0].value = make([]byte, len(value))
				copy(leftBoundary.next[0].value, value)
				leftBoundary.mu.Unlock()
				return nil
			}
			leftBoundary.mu.Unlock()
			// 如果验证失败，重试
			continue
		}

		// 第三步：插入新节点
		newLevel := s.GetRandomLevel()

		// 确保新层级不超过最大值
		if newLevel > MAX_LEVEL {
			newLevel = MAX_LEVEL
		}

		// 确定需要锁定的层数范围
		s.mu.Lock()
		lockLevels := newLevel
		if newLevel > s.level {
			// 如果新层数超过当前最大层数，需要更新
			for i := s.level; i < newLevel && i < MAX_LEVEL; i++ {
				update[i] = s.head
			}
			s.level = newLevel
		}
		s.mu.Unlock()

		// 第四步：按顺序锁定左边界节点（避免死锁）
		// 收集需要锁定的节点，去重
		var lockedNodes []*skipListNode
		lockedMap := make(map[*skipListNode]bool)

		// 按照节点地址排序锁定，避免死锁
		for i := 0; i < lockLevels && i < MAX_LEVEL; i++ {
			node := update[i]
			if node != nil && !lockedMap[node] {
				lockedNodes = append(lockedNodes, node)
				lockedMap[node] = true
			}
		}

		// 锁定所有左边界节点
		for _, node := range lockedNodes {
			node.mu.Lock()
		}

		// 第五步：验证锁定后的状态是否依然有效
		valid := true
		for i := 0; i < lockLevels && i < MAX_LEVEL && valid; i++ {
			leftBoundary := update[i]
			expectedNext := next[i]

			// 验证链接关系是否依然正确
			if leftBoundary.next[i] != expectedNext {
				valid = false
				break
			}

			// 验证是否有其他线程插入了相同的key
			if expectedNext != nil && bytes.Equal(expectedNext.key, key) {
				valid = false
				break
			}
		}

		if !valid {
			// 释放锁并重试
			for i := len(lockedNodes) - 1; i >= 0; i-- {
				lockedNodes[i].mu.Unlock()
			}
			continue
		}

		// 第六步：执行插入操作
		newNode := &skipListNode{
			key:   make([]byte, len(key)),
			value: make([]byte, len(value)),
			next:  make([]*skipListNode, newLevel),
		}
		copy(newNode.key, key)
		copy(newNode.value, value)

		// 更新指针
		for i := 0; i < lockLevels && i < MAX_LEVEL; i++ {
			newNode.next[i] = update[i].next[i]
			update[i].next[i] = newNode
		}

		// 更新统计信息
		s.mu.Lock()
		s.entriesCnt++
		s.size += len(key) + len(value)
		s.mu.Unlock()

		// 第七步：释放所有锁
		for i := len(lockedNodes) - 1; i >= 0; i-- {
			lockedNodes[i].mu.Unlock()
		}

		return nil
	}
}

func (s *SkipList) GetRandomLevel() int {
	level := 1
	for rand.Float32() < P && level < MAX_LEVEL {
		level++
	}
	return level
}

// searchInternal 内部搜索，不加锁
func (s *SkipList) searchInternal(key []byte) *skipListNode {
	s.mu.RLock()
	currentLevel := s.level
	s.mu.RUnlock()

	cur := s.head
	for i := currentLevel - 1; i >= 0; i-- {
		for cur.next[i] != nil && bytes.Compare(cur.next[i].key, key) < 0 {
			cur = cur.next[i]
		}
		if cur.next[i] != nil && bytes.Equal(cur.next[i].key, key) {
			return cur.next[i]
		}
	}
	return nil
}

func (s *SkipList) Search(key []byte) *skipListNode {
	return s.searchInternal(key)
}

func (s *SkipList) Get(key []byte) ([]byte, bool) {
	// 找到左边界节点
	update, _ := s.findLeftBoundary(key)
	leftBoundary := update[0]

	// 锁定左边界节点以确保读取一致性
	leftBoundary.mu.RLock()
	defer leftBoundary.mu.RUnlock()

	// 检查下一个节点是否是目标节点
	if leftBoundary.next[0] != nil && bytes.Equal(leftBoundary.next[0].key, key) {
		target := leftBoundary.next[0]
		// 复制值以避免竞态条件
		value := make([]byte, len(target.value))
		copy(value, target.value)
		return value, true
	}
	return nil, false
}

func (s *SkipList) All() []*KVPair {
	s.mu.RLock()
	entriesCount := s.entriesCnt
	s.mu.RUnlock()

	if entriesCount == 0 {
		return nil
	}

	// 锁定头节点以遍历level 0链表
	s.head.mu.RLock()
	defer s.head.mu.RUnlock()

	nodes := make([]*KVPair, 0, entriesCount)
	cur := s.head

	// 遍历最底层的所有节点
	for cur.next[0] != nil {
		cur = cur.next[0]
		// 复制数据
		key := make([]byte, len(cur.key))
		value := make([]byte, len(cur.value))
		copy(key, cur.key)
		copy(value, cur.value)

		nodes = append(nodes, &KVPair{
			Key:   key,
			Value: value,
		})
	}

	return nodes
}
