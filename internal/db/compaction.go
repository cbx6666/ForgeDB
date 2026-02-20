package db

import (
	"container/heap"
	"fmt"
	"os"
	"path/filepath"

	"monolithdb/internal/sstable"
	"monolithdb/internal/types"
)

// 堆元素：某个 SST 的当前记录 + 该 SST 的新旧优先级
type heapItem struct {
	it       *sstable.Iter
	ent      types.Entry
	priority int
}

type mergeHeap []heapItem

func (h mergeHeap) Len() int { return len(h) }

func (h mergeHeap) Less(i, j int) bool {
	ki, kj := h[i].ent.Key, h[j].ent.Key
	if ki != kj {
		return ki < kj
	}
	return h[i].priority < h[j].priority
}

func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *mergeHeap) Push(x any) { *h = append(*h, x.(heapItem)) }

func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// CompactL0ToL1 将 L0 的所有 SST 与 L1（通常 0/1 个）合并，输出一个新的 L1 SST。
// 合并规则：同 key 只保留最新版本（按 newest-first 决定），tombstone 也会保留。
func (d *DB) CompactL0ToL1() error {
	if len(d.l0) == 0 {
		return nil
	}

	// inputs：L0 在前（prio 小、更“新”），L1 在后（prio 大、更“旧”）
	inputs := make([]string, 0, len(d.l0)+len(d.l1))
	inputs = append(inputs, d.l0...)
	inputs = append(inputs, d.l1...)

	h := &mergeHeap{}
	heap.Init(h)

	// 统一关闭迭代器
	var iters []*sstable.Iter
	defer func() {
		for _, it := range iters {
			_ = it.Close()
		}
	}()

	// 初始化堆
	for prio, path := range inputs {
		it, err := sstable.NewIter(path)
		if err != nil {
			return err
		}
		iters = append(iters, it)

		if it.Valid() {
			heap.Push(h, heapItem{
				it:       it,
				ent:      it.Entry(),
				priority: prio,
			})
		}
	}

	// 多路归并：同 key 只取最新
	merged := make([]types.Entry, 0, 1024)
	var lastKey string
	hasLast := false

	for h.Len() > 0 {
		item := heap.Pop(h).(heapItem)

		if !hasLast || item.ent.Key != lastKey {
			merged = append(merged, item.ent)
			lastKey = item.ent.Key
			hasLast = true
		}

		if err := item.it.Next(); err != nil {
			return err
		}
		if item.it.Valid() {
			item.ent = item.it.Entry()
			heap.Push(h, item)
		}
	}

	// 写新 L1（tmp -> rename）
	name := fmt.Sprintf("%06d.sst", d.nextID)
	finalPath := filepath.Join(d.l1Dir, name)
	tmpPath := finalPath + ".tmp"

	if err := sstable.WriteTable(tmpPath, merged); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}

	// 先保存旧文件列表，最后统一删除
	oldL0 := d.l0
	oldL1 := d.l1

	// 更新内存状态：L0 清空，L1 替换成新文件（newest-first）
	d.l0 = nil
	d.l1 = []string{finalPath}
	d.nextID++

	// 最后删旧文件（避免中途出错丢数据）
	for _, p := range oldL0 {
		_ = os.Remove(p)
	}
	for _, p := range oldL1 {
		_ = os.Remove(p)
	}

	return nil
}
