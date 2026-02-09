package memtable

import "monolithdb/internal/types"

// MemTable 是数据库的内存表：对外提供 Put/Get/Delete/Range。
// 内部用 SkipList 存储有序 key。
type MemTable struct {
	sl *SkipList
}

func NewMemTable() *MemTable {
	return &MemTable{sl: NewSkipList()}
}

// Put 写入/更新：本质是对 SkipList 做 Upsert。
func (m *MemTable) Put(key string, value []byte) {
	e := types.Entry{
		Key:       key,
		Value:     cloneBytes(value),
		Tombstone: false,
	}

	m.sl.Upsert(key, e)
}

// Get 查询：先从 SkipList.Search 拿到 Entry，再处理 tombstone。
func (m *MemTable) Get(key string) ([]byte, bool) {
	e, ok := m.sl.Search(key)
	if !ok || e.Tombstone {
		return nil, false
	}

	return cloneBytes(e.Value), true
}

// Delete 删除：写 tombstone 覆盖
func (m *MemTable) Delete(key string) {
	e := types.Entry{
		Key:       key,
		Value:     nil,
		Tombstone: true,
	}

	m.sl.Upsert(key, e)
}

// Range 范围查询：返回 [start, end) 的有序记录。
func (m *MemTable) Range(start, end string) []types.Entry {
	var out []types.Entry

	var n *node
	if start == "" {
		n = m.sl.First()
	} else {
		n = m.sl.FirstGE(start)
	}

	for n != nil && (end == "" || n.key < end) {
		if !n.entry.Tombstone {
			out = append(out, types.Entry{
				Key:       n.key,
				Value:     cloneBytes(n.entry.Value),
				Tombstone: false,
			})
		}
		n = n.forward[0]
	}

	return out
}

// RangeAll 范围查询：返回 [start, end) 的有序记录（包含 tombstone）。
// 用于 Flush 到 SSTable，保证 Delete 也会被持久化。
func (m *MemTable) RangeAll(start, end string) []types.Entry {
	var out []types.Entry

	var n *node
	if start == "" {
		n = m.sl.First()
	} else {
		n = m.sl.FirstGE(start)
	}

	for n != nil && (end == "" || n.key < end) {
		// 这里不跳过 tombstone
		out = append(out, types.Entry{
			Key:       n.key,
			Value:     cloneBytes(n.entry.Value),
			Tombstone: n.entry.Tombstone,
		})

		n = n.forward[0]
	}

	return out
}

// cloneBytes 防御性拷贝，避免外部修改 slice 影响表内数据。
func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}

	cp := make([]byte, len(b))
	copy(cp, b)
	return cp
}
