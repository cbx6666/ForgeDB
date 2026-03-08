package db

import (
	"container/heap"
	"os"
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

type compactionJob struct {
	level int

	srcPaths []string
	dstPaths []string

	minKey string
	maxKey string
}

type compactionResult struct {
	job     *compactionJob
	newRuns []levelFile
}

func (d *DB) RequestCompaction() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.checkBGErrLocked(); err != nil {
		return err
	}
	d.requestCompactionLocked()
	return nil
}

func (d *DB) pickCompactionJobLocked(level int) (*compactionJob, error) {
	if level < 0 || level+1 >= len(d.levels) {
		return nil, nil
	}

	src := &d.levels[level]
	dst := &d.levels[level+1]

	// src 为空直接返回
	if level == 0 {
		if len(src.l0Paths) == 0 {
			return nil, nil
		}
	} else {
		if len(src.runs) == 0 {
			return nil, nil
		}
	}

	pickN := d.opt.levels[level].pickN
	if pickN <= 0 {
		return nil, nil
	}

	// 1) pick inputs from src
	var pickedPaths []string
	if level == 0 {
		picked, _ := pickOldestL0(src.l0Paths, pickN, d.isPendingLocked)
		pickedPaths = picked
	} else {
		pickedRuns, _ := pickOldestRuns(src.runs, pickN, d.isPendingLocked)
		for _, f := range pickedRuns {
			pickedPaths = append(pickedPaths, f.path)
		}
	}
	if len(pickedPaths) == 0 {
		return nil, nil
	}

	// 2) range of picked
	minK, maxK, err := rangeOfFiles(pickedPaths)
	if err != nil {
		return nil, err
	}

	// 3) overlap in dst (基于当前 dst.runs)
	overIdx := overlappedRuns(dst.runs, minK, maxK)
	dstPaths := make([]string, 0, len(overIdx))
	for _, i := range overIdx {
		p := dst.runs[i].path
		if d.isPendingLocked(p) {
			return nil, nil
		}

		dstPaths = append(dstPaths, p)
	}

	// 4) 标记 pending
	all := make([]string, 0, len(pickedPaths)+len(dstPaths))
	all = append(all, pickedPaths...)
	all = append(all, dstPaths...)
	d.markPendingLocked(all)

	return &compactionJob{
		level:    level,
		srcPaths: append([]string(nil), pickedPaths...),
		dstPaths: dstPaths,
		minKey:   minK,
		maxKey:   maxK,
	}, nil
}

func (d *DB) doCompaction(job *compactionJob) (*compactionResult, error) {
	inputs := make([]string, 0, len(job.srcPaths)+len(job.dstPaths))
	inputs = append(inputs, job.srcPaths...)
	inputs = append(inputs, job.dstPaths...)

	merged, err := mergeIters(inputs)
	if err != nil {
		return nil, err
	}
	if len(merged) == 0 {
		return &compactionResult{job: job, newRuns: nil}, nil
	}

	newRuns, _, err := d.writeRuns(job.level+1, merged)
	if err != nil {
		return nil, err
	}
	return &compactionResult{job: job, newRuns: newRuns}, nil
}

func (d *DB) installCompactionLocked(res *compactionResult) error {
	job := res.job
	level := job.level
	if level < 0 || level+1 >= len(d.levels) {
		return nil
	}

	src := &d.levels[level]
	dst := &d.levels[level+1]

	// 1) 从 src 移除 job.srcPaths
	if level == 0 {
		src.l0Paths = removePaths(src.l0Paths, job.srcPaths)
	} else {
		src.runs = removeRunsByPaths(src.runs, job.srcPaths)
	}

	// 2) 重新算 overlap（用 job.minKey/maxKey，而不是用旧的 dstPaths 快照）
	overIdx := overlappedRuns(dst.runs, job.minKey, job.maxKey)
	realOldDst := make([]string, 0, len(overIdx))
	for _, i := range overIdx {
		realOldDst = append(realOldDst, dst.runs[i].path)
	}

	// 3) 替换 dst 的 overlap 区间
	newDst := make([]levelFile, 0, len(dst.runs)-len(overIdx)+len(res.newRuns))
	if len(overIdx) == 0 {
		newDst = append(newDst, dst.runs...)
		newDst = append(newDst, res.newRuns...)
		sort.Slice(newDst, func(i, j int) bool { return newDst[i].minKey < newDst[j].minKey })
	} else {
		start := overIdx[0]
		end := overIdx[len(overIdx)-1] + 1
		newDst = append(newDst, dst.runs[:start]...)
		newDst = append(newDst, res.newRuns...)
		newDst = append(newDst, dst.runs[end:]...)
	}
	dst.runs = newDst

	// 4) persist manifest
	if err := d.persistManifest(); err != nil {
		return err
	}

	// 5) 删除旧文件：srcPaths + realOldDst
	for _, p := range job.srcPaths {
		_ = os.Remove(p)
	}
	for _, p := range realOldDst {
		_ = os.Remove(p)
	}

	// 6) 删除标记
	all := make([]string, 0, len(job.srcPaths)+len(realOldDst))
	all = append(all, job.srcPaths...)
	all = append(all, realOldDst...)
	d.clearPendingLocked(all)

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

func (d *DB) pickCompactionLevelLocked() (int, bool) {
	// 从低层往高层找第一个超阈值的
	for level := 0; level < len(d.levels)-1; level++ {
		maxFiles := d.opt.levels[level].maxFiles
		if maxFiles <= 0 {
			continue
		}

		over := false
		if level == 0 {
			over = len(d.levels[level].l0Paths) > maxFiles
		} else {
			over = len(d.levels[level].runs) > maxFiles
		}

		if over {
			return level, true
		}
	}
	return 0, false
}

func (d *DB) isPendingLocked(path string) bool {
	_, ok := d.pending[path]
	return ok
}

func (d *DB) markPendingLocked(paths []string) {
	for _, p := range paths {
		d.pending[p] = struct{}{}
	}
}

func (d *DB) clearPendingLocked(paths []string) {
	for _, p := range paths {
		delete(d.pending, p)
	}
}
