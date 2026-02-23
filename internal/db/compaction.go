package db

import (
	"container/heap"
	"fmt"
	"os"
	"path/filepath"
	"sort"

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

// CompactL0ToL1
// - 每次只选择最老的 l0PickN 个 L0 文件参与 compaction
// - 只合并与选中 L0 key-range 重叠的那段 L1 文件
// - 输出 1 个新的 L1 文件，并只替换 L1 的 overlap 区间
func (d *DB) CompactL0ToL1() error {
	if len(d.l0) == 0 {
		return nil
	}

	// 1) 选最老的 N 个 L0
	pickedL0, remainL0 := pickOldestL0(d.l0, l0PickN)
	if len(pickedL0) == 0 {
		return nil
	}

	// 2) 计算 pickedL0 的 key-range
	minK, maxK, err := rangeOfFiles(pickedL0)
	if err != nil {
		return err
	}

	// 3) 找 overlap 的 L1（返回 index 列表）
	overIdx := overlappedL1(d.l1, minK, maxK)

	// 4) inputs：pickedL0 在前（prio 小，更新），overlap L1 在后
	inputs := make([]string, 0, len(pickedL0)+len(overIdx))
	inputs = append(inputs, pickedL0...)
	for _, i := range overIdx {
		inputs = append(inputs, d.l1[i].path)
	}

	// 5) heap merge
	h := &mergeHeap{}
	heap.Init(h)

	var iters []*sstable.Iter
	defer func() {
		for _, it := range iters {
			_ = it.Close()
		}
	}()

	for prio, path := range inputs {
		it, err := sstable.NewIter(path)
		if err != nil {
			return err
		}
		iters = append(iters, it)
		if it.Valid() {
			heap.Push(h, heapItem{it: it, ent: it.Entry(), priority: prio})
		}
	}

	merged := make([]types.Entry, 0, 1024)
	var lastKey string
	hasLast := false

	for h.Len() > 0 {
		item := heap.Pop(h).(heapItem)

		// 同 key 只保留最先弹出的（priority 最小那条）
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

	// 如果 merged 为空：说明 pickedL0/overlapL1 都是空表（理论上不该发生），直接返回
	if len(merged) == 0 {
		return nil
	}

	// 6) 写新的 L1 runs（多文件，tmp -> rename）
	newRuns, _, err := d.writeL1Runs(merged)
	if err != nil {
		return err
	}
	if len(newRuns) == 0 {
		return nil
	}

	// 7) 构造新 L1：只替换 overlap 区间（插入多个 runs）
	newL1 := make([]levelFile, 0, len(d.l1)-len(overIdx)+len(newRuns))
	if len(overIdx) == 0 {
		// 没 overlap：插入后排序（保持 minKey 升序）
		newL1 = append(newL1, d.l1...)
		newL1 = append(newL1, newRuns...)
		sort.Slice(newL1, func(i, j int) bool { return newL1[i].minKey < newL1[j].minKey })
	} else {
		start := overIdx[0]
		end := overIdx[len(overIdx)-1] + 1
		newL1 = append(newL1, d.l1[:start]...)
		newL1 = append(newL1, newRuns...)
		newL1 = append(newL1, d.l1[end:]...)
	}

	// 8) 先保存要删除的输入文件，再更新内存状态
	oldL0 := pickedL0
	oldL1 := make([]string, 0, len(overIdx))
	for _, i := range overIdx {
		oldL1 = append(oldL1, d.l1[i].path)
	}

	d.l0 = remainL0
	d.l1 = newL1

	// 9) 最后删除旧文件
	for _, p := range oldL0 {
		_ = os.Remove(p)
	}
	for _, p := range oldL1 {
		_ = os.Remove(p)
	}

	return nil
}

func (d *DB) writeL1Runs(merged []types.Entry) ([]levelFile, []string, error) {
	// 返回：
	// - newRuns：新生成的 L1 文件信息（minKey/maxKey）
	// - createdPaths：实际创建的文件路径（方便出错时清理）
	if len(merged) == 0 {
		return nil, nil, nil
	}

	newRuns := make([]levelFile, 0, (len(merged)+l1RunMaxEntries-1)/l1RunMaxEntries)
	created := make([]string, 0, cap(newRuns))

	for i := 0; i < len(merged); {
		j := i + l1RunMaxEntries
		if j > len(merged) {
			j = len(merged)
		}
		chunk := merged[i:j]

		name := fmt.Sprintf("%06d.sst", d.nextID)
		finalPath := filepath.Join(d.l1Dir, name)
		tmpPath := finalPath + ".tmp"

		if err := sstable.WriteTable(tmpPath, chunk); err != nil {
			_ = os.Remove(tmpPath)
			// 清理已创建的 run
			for _, p := range created {
				_ = os.Remove(p)
			}
			return nil, nil, err
		}
		if err := os.Rename(tmpPath, finalPath); err != nil {
			_ = os.Remove(tmpPath)
			for _, p := range created {
				_ = os.Remove(p)
			}
			return nil, nil, err
		}

		created = append(created, finalPath)
		newRuns = append(newRuns, levelFile{
			path:   finalPath,
			minKey: chunk[0].Key,
			maxKey: chunk[len(chunk)-1].Key,
		})

		d.nextID++
		i = j
	}

	return newRuns, created, nil
}
