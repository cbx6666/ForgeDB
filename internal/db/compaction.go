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

// CompactFull 将当前所有 SSTables（newest-first）合并为一个新的 SSTable。
// 合并规则：同 key 只保留最新版本（按 newest-first 决定），tombstone 也会保留。
func (d *DB) CompactFull() error {
	if len(d.sstables) <= 1 {
		return nil
	}

	h := &mergeHeap{}
	heap.Init(h)

	// 统一关闭迭代器
	var iters []*sstable.Iter
	defer func() {
		for _, it := range iters {
			_ = it.Close()
		}
	}()

	// 1) 初始化堆：每个 SST 放入当前 record
	for prio, path := range d.sstables { // prio=0 最新
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

	// 2) 多路归并：key 升序输出；同 key 保留 newest-first 的那条
	merged := make([]types.Entry, 0, 1024)
	lastKey := ""

	for h.Len() > 0 {
		item := heap.Pop(h).(heapItem)

		// 去重：新版本的优先级更高，同 key 的旧版本跳过
		if item.ent.Key != lastKey {
			merged = append(merged, item.ent)
			lastKey = item.ent.Key
		}

		// 推进该 iterator
		if err := item.it.Next(); err != nil {
			return err
		}
		if item.it.Valid() {
			item.ent = item.it.Entry()
			heap.Push(h, item)
		}
	}

	// 3) 写新 SST（临时文件 -> rename）
	name := fmt.Sprintf("%06d.sst", d.nextID)
	finalPath := filepath.Join(d.sstDir, name)
	tmpPath := finalPath + ".tmp"

	if err := sstable.WriteTable(tmpPath, merged); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}

	// 4) 替换 sstables：只保留新表（newest-first）
	old := d.sstables
	d.sstables = []string{finalPath}
	d.nextID++

	// 5) 删除旧表（最后删，避免中途出错丢数据）
	for _, p := range old {
		_ = os.Remove(p)
	}

	return nil
}
