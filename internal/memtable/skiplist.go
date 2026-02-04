package memtable

import (
	"math/rand"
	"time"

	"monolithdb/internal/types"
)

const (
	maxLevel = 16
)

type node struct {
	key     string
	entry   types.Entry
	forward []*node
}

// SkipList 是跳表结构，提供比链表更快的访问方法
// head 是虚拟头节点，不存真实 key
// level 表示当前跳表实际使用的层数（从 1 开始），越高节点越稀疏
type SkipList struct {
	head  *node
	level int
	rnd   *rand.Rand
}

func NewSkipList() *SkipList {
	h := &node{
		forward: make([]*node, maxLevel),
	}
	return &SkipList{
		head:  h,
		level: 1,
		rnd:   rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (s *SkipList) randomLevel() int {
	lvl := 1
	for lvl < maxLevel && s.rnd.Intn(2) == 0 {
		lvl++
	}

	return lvl
}

func (s *SkipList) Search(key string) (types.Entry, bool) {
	x := s.head

	for i := s.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && x.forward[i].key < key {
			x = x.forward[i]
		}
	}

	// 最后在第 0 层确认
	x = x.forward[0]
	if x != nil && x.key == key {
		return x.entry, true
	}
	return types.Entry{}, false
}

func (s *SkipList) Upsert(key string, entry types.Entry) {
	update := make([]*node, maxLevel)

	x := s.head
	// 找到每层的前驱
	for i := s.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && x.forward[i].key < key {
			x = x.forward[i]
		}
		update[i] = x
	}

	// 检查 level0 的下一个是不是目标 key
	x = x.forward[0]
	if x != nil && x.key == key {
		x.entry = entry
		return
	}

	// 生成新节点层高，通过随机使高层节点稀疏
	lvl := s.randomLevel()

	if lvl > s.level {
		for i := s.level; i < lvl; i++ {
			update[i] = s.head
		}
		s.level = lvl
	}

	newNode := &node{
		key:     key,
		entry:   entry,
		forward: make([]*node, lvl),
	}

	for i := 0; i < lvl; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}
}

func (s *SkipList) First() *node {
	return s.head.forward[0]
}

// 返回第一个 key >= target 的节点
func (s *SkipList) FirstGE(target string) *node {
	x := s.head

	for i := s.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && x.forward[i].key < target {
			x = x.forward[i]
		}
	}
	return x.forward[0]
}
