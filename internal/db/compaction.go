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

func (d *DB) Compact(level int) error {
	if level < 0 || level+1 >= len(d.levels) {
		return nil
	}

	src := &d.levels[level]
	dst := &d.levels[level+1]

	// src 为空直接返回
	if level == 0 {
		if len(src.l0Paths) == 0 {
			return nil
		}
	} else {
		if len(src.runs) == 0 {
			return nil
		}
	}

	// 1) pick inputs from src
	var pickedPaths []string
	commitSrc := func() {}
	pickN := d.opt.levels[level].pickN
	if pickN <= 0 {
		return nil
	}

	if level == 0 {
		picked, remain := pickOldestL0(src.l0Paths, pickN)
		if len(picked) == 0 {
			return nil
		}
		pickedPaths = picked
		commitSrc = func() { src.l0Paths = remain }
	} else {
		pickedRuns, remainRuns := pickOldestRuns(src.runs, pickN)
		if len(pickedRuns) == 0 {
			return nil
		}
		for _, f := range pickedRuns {
			pickedPaths = append(pickedPaths, f.path)
		}
		commitSrc = func() { src.runs = remainRuns }
	}

	// 2) range of picked
	minK, maxK, err := rangeOfFiles(pickedPaths)
	if err != nil {
		return err
	}

	// 3) overlap in dst (dst 是 L>=1 runs)
	overIdx := overlappedRuns(dst.runs, minK, maxK)

	// 4) build inputs: pickedPaths first, overlapped dst after
	inputs := make([]string, 0, len(pickedPaths)+len(overIdx))
	inputs = append(inputs, pickedPaths...)
	for _, i := range overIdx {
		inputs = append(inputs, dst.runs[i].path)
	}

	// 5) heap merge
	merged, err := mergeIters(inputs)
	if err != nil {
		return err
	}
	if len(merged) == 0 {
		return nil
	}

	// 6) writeRuns(level+1)
	newRuns, _, err := d.writeRuns(level+1, merged)
	if err != nil {
		return err
	}
	if len(newRuns) == 0 {
		return nil
	}

	// 7) replace overlap in dst
	newDst := make([]levelFile, 0, len(dst.runs)-len(overIdx)+len(newRuns))
	if len(overIdx) == 0 {
		newDst = append(newDst, dst.runs...)
		newDst = append(newDst, newRuns...)
		sort.Slice(newDst, func(i, j int) bool { return newDst[i].minKey < newDst[j].minKey })
	} else {
		start := overIdx[0]
		end := overIdx[len(overIdx)-1] + 1
		newDst = append(newDst, dst.runs[:start]...)
		newDst = append(newDst, newRuns...)
		newDst = append(newDst, dst.runs[end:]...)
	}

	// 8) collect old files to delete (MUST before install)
	oldSrc := append([]string(nil), pickedPaths...)

	oldDst := make([]string, 0, len(overIdx))
	for _, i := range overIdx {
		oldDst = append(oldDst, dst.runs[i].path)
	}

	// 9) install metadata (atomic in-memory install)
	commitSrc()
	dst.runs = newDst

	if err := d.persistManifest(); err != nil {
		return err
	}

	// 10) delete old files
	for _, p := range oldSrc {
		_ = os.Remove(p)
	}
	for _, p := range oldDst {
		_ = os.Remove(p)
	}

	return nil
}

func mergeIters(paths []string) ([]types.Entry, error) {
	h := &mergeHeap{}
	heap.Init(h)

	var iters []*sstable.Iter
	defer func() {
		for _, it := range iters {
			_ = it.Close()
		}
	}()

	for prio, path := range paths {
		it, err := sstable.NewIter(path)
		if err != nil {
			return nil, err
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
			return nil, err
		}
		if item.it.Valid() {
			item.ent = item.it.Entry()
			heap.Push(h, item)
		}
	}

	return merged, nil
}

func (d *DB) writeRuns(dstLevel int, merged []types.Entry) ([]levelFile, []string, error) {
	if len(merged) == 0 {
		return nil, nil, nil
	}

	runMax := d.opt.levels[dstLevel].runMaxEntries
	if runMax <= 0 {
		return nil, nil, fmt.Errorf("db: invalid runMaxEntries for level %d", dstLevel)
	}
	newRuns := make([]levelFile, 0, (len(merged)+runMax-1)/runMax)
	created := make([]string, 0, cap(newRuns))

	for i := 0; i < len(merged); {
		j := i + runMax
		if j > len(merged) {
			j = len(merged)
		}
		chunk := merged[i:j]

		name := fmt.Sprintf("%06d.sst", d.nextID)
		finalPath := filepath.Join(d.levels[dstLevel].dir, name)
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
			id:     d.nextID,
			path:   finalPath,
			minKey: chunk[0].Key,
			maxKey: chunk[len(chunk)-1].Key,
		})

		d.nextID++
		i = j
	}

	return newRuns, created, nil
}
