package db

import (
	"container/heap"

	"monolithdb/internal/sstable"
	"monolithdb/internal/types"
)

// db_compaction_run.go 负责 compaction 的归并执行和新 run 产出逻辑。

// heapItem 表示某个 SST 当前迭代到的记录，以及这一路输入的优先级。
type heapItem struct {
	it       *sstable.Iter
	ent      types.Entry
	priority int
}

type mergeHeap []heapItem

// Len 返回堆中当前元素数量。
func (h mergeHeap) Len() int { return len(h) }

// Less 定义最小堆顺序：先按 key，再按输入优先级。
func (h mergeHeap) Less(i, j int) bool {
	ki, kj := h[i].ent.Key, h[j].ent.Key
	if ki != kj {
		return ki < kj
	}
	return h[i].priority < h[j].priority
}

// Swap 交换堆中两个元素的位置。
func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

// Push 向堆中压入一个新的候选项。
func (h *mergeHeap) Push(x any) { *h = append(*h, x.(heapItem)) }

// Pop 从堆中弹出当前最小的候选项。
func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// doCompaction 执行一次 compaction，并生成新的 run 集合。
func (d *DB) doCompaction(job *compactionJob) ([]levelFile, error) {
	// 目标层已经是最后一层时，后面不再有更老数据需要被 tombstone 遮蔽，
	// 因此可以直接在这轮 compaction 中丢弃删除标记。
	dstLevel := job.dstLevel()
	dropBottomTombstones := dstLevel == d.opt.numLevels-1
	newRuns, err := d.streamCompactionRuns(dstLevel, job.allPaths(), dropBottomTombstones)
	if err != nil {
		return nil, err
	}
	return newRuns, nil
}

// streamCompactionRuns 以流式方式归并多个 SST 输入，并分段写出目标层 run。
func (d *DB) streamCompactionRuns(dstLevel int, inputs []string, dropBottomTombstones bool) (outRuns []levelFile, err error) {
	runMax := d.opt.levels[dstLevel].runMaxEntries
	if runMax <= 0 {
		return nil, nil
	}

	h := &mergeHeap{}
	heap.Init(h)

	var iters []*sstable.Iter
	defer func() {
		for _, it := range iters {
			_ = it.Close()
		}
	}()

	created := make([]string, 0, len(inputs))
	defer func() {
		if err == nil {
			return
		}
		// 一旦中途失败，删除已经写出的 compaction 产物，避免留下孤儿文件。
		for _, p := range created {
			_ = removeFileAndSync(p)
		}
	}()

	for prio, path := range inputs {
		// 为每个输入 SST 打开顺序迭代器，并记录输入优先级。
		// priority 越小，表示这一路输入越“新”，同 key 去重时应优先保留。
		it, openErr := sstable.NewIter(path)
		if openErr != nil {
			err = openErr
			return nil, err
		}
		iters = append(iters, it)
		if it.Valid() {
			// 只把当前有效项放进堆，后续靠迭代器推进维持多路归并。
			heap.Push(h, heapItem{it: it, ent: it.Entry(), priority: prio})
		}
	}

	outRuns = make([]levelFile, 0, len(inputs))
	chunk := make([]types.Entry, 0, runMax)

	flushChunk := func() error {
		if len(chunk) == 0 {
			return nil
		}
		// chunk 的大小已经被 runMax 限住了，因此这里一次只写一个目标 run。
		run, made, writeErr := d.writeSingleRun(dstLevel, chunk)
		if writeErr != nil {
			return writeErr
		}
		outRuns = append(outRuns, run)
		created = append(created, made)
		chunk = chunk[:0]
		return nil
	}

	emit := func(ent types.Entry) error {
		// 到达最后一层时，墓碑不需要继续向后传播，可以直接丢弃。
		if dropBottomTombstones && ent.Tombstone {
			return nil
		}
		chunk = append(chunk, ent)
		if len(chunk) < runMax {
			// 还没达到目标 run 大小上限时继续聚合。
			return nil
		}
		return flushChunk()
	}

	var lastKey string
	hasLast := false

	for h.Len() > 0 {
		// 每次取出当前全局最小 key；如果 key 相同，则由 priority 保证
		// “较新的输入先出堆”，从而完成跨文件去重。
		item := heap.Pop(h).(heapItem)

		if !hasLast || item.ent.Key != lastKey {
			// 只有遇到一个新 key 时才真正输出一次。
			if err := emit(item.ent); err != nil {
				return nil, err
			}
			lastKey = item.ent.Key
			hasLast = true
		}

		// 推进当前输入迭代器，并把下一个候选项重新放回堆中。
		if nextErr := item.it.Next(); nextErr != nil {
			err = nextErr
			return nil, err
		}
		if item.it.Valid() {
			item.ent = item.it.Entry()
			heap.Push(h, item)
		}
	}

	// 刷出最后一个未满的分段。
	if err := flushChunk(); err != nil {
		return nil, err
	}
	return outRuns, nil
}
