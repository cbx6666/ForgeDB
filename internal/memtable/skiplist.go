package memtable

import "monolithdb/internal/types"

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
}

func NewSkipList() *SkipList {
	h := &node{
		forward: make([]*node, maxLevel),
	}
	return &SkipList{
		head:  h,
		level: 1,
	}
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

func (s *SkipList) Update(key string, entry types.Entry) {
	prev := s.head
	cur := prev.forward[0]

	for cur != nil && cur.key < key {
		prev = cur
		cur = cur.forward[0]
	}

	if cur != nil && cur.key == key {
		cur.entry = entry
	}

	newNode := &node{
		key:     key,
		entry:   entry,
		forward: make([]*node, 1),
	}
	newNode.forward[0] = cur
	prev.forward[0] = newNode
}


