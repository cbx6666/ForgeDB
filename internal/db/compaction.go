package db

import (
	"container/heap"
	"sort"

	"monolithdb/internal/sstable"
	"monolithdb/internal/types"
)

// heapItem 表示某个 SST 当前迭代到的记录，以及这一路输入的优先级。
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
	v := d.currentVersion()
	if level < 0 || level+1 >= len(v.levels) {
		return nil, nil
	}

	src := &v.levels[level]
	dst := &v.levels[level+1]

	// 源层为空则没有可做的 compaction。
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

	// 1) 从源层挑一批输入文件。
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

	// 2) 计算这批输入覆盖的整体 key 范围。
	minK, maxK, err := rangeOfFiles(pickedPaths)
	if err != nil {
		return nil, err
	}

	// 3) 找到目标层里和这段范围有重叠的文件。
	overIdx := overlappedRuns(dst.runs, minK, maxK)
	dstPaths := make([]string, 0, len(overIdx))
	for _, i := range overIdx {
		p := dst.runs[i].path
		if d.isPendingLocked(p) {
			return nil, nil
		}

		dstPaths = append(dstPaths, p)
	}

	// 4) 标记 pending，避免后台并发重复选中。
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

	// 目标层已经是最后一层时，后面不再有更老的数据需要被 tombstone 遮蔽，
	// 因此可以在这次 compaction 中直接丢弃删除标记。
	dropBottomTombstones := job.level+1 == d.opt.numLevels-1
	newRuns, _, err := d.streamCompactionRuns(job.level+1, inputs, dropBottomTombstones)
	if err != nil {
		return nil, err
	}
	return &compactionResult{job: job, newRuns: newRuns}, nil
}

func (d *DB) streamCompactionRuns(dstLevel int, inputs []string, dropBottomTombstones bool) ([]levelFile, []string, error) {
	runMax := d.opt.levels[dstLevel].runMaxEntries
	if runMax <= 0 {
		return nil, nil, nil
	}

	h := &mergeHeap{}
	heap.Init(h)

	var iters []*sstable.Iter
	defer func() {
		for _, it := range iters {
			_ = it.Close()
		}
	}()

	for prio, path := range inputs {
		// 为每个输入 SST 打开顺序迭代器，并按输入顺序记录优先级。
		// priority 越小，表示这一路输入越“新”，同 key 去重时应优先保留。
		it, err := sstable.NewIter(path)
		if err != nil {
			return nil, nil, err
		}
		iters = append(iters, it)
		if it.Valid() {
			heap.Push(h, heapItem{it: it, ent: it.Entry(), priority: prio})
		}
	}

	outRuns := make([]levelFile, 0, len(inputs))
	created := make([]string, 0, len(inputs))
	chunk := make([]types.Entry, 0, runMax)

	cleanupCreated := func() {
		// 一旦中途失败，删除已经写出的 compaction 产物，避免留下孤儿文件。
		for _, p := range created {
			_ = removeFileAndSync(p)
		}
	}

	flushChunk := func() error {
		if len(chunk) == 0 {
			return nil
		}
		// 这里的 chunk 大小已经被 runMax 限住了，因此只需要写一个 run。
		// 流式 compaction 自己负责分段，底层只保留“写单个 run”的职责。
		run, made, err := d.writeSingleRun(dstLevel, chunk)
		if err != nil {
			cleanupCreated()
			return err
		}
		outRuns = append(outRuns, run)
		created = append(created, made)
		chunk = chunk[:0]
		return nil
	}

	emit := func(ent types.Entry) error {
		// 到达最后一层时，墓碑已经不需要再向后传播，可以直接丢弃。
		if dropBottomTombstones && ent.Tombstone {
			return nil
		}
		chunk = append(chunk, ent)
		if len(chunk) < runMax {
			return nil
		}
		return flushChunk()
	}

	var lastKey string
	hasLast := false

	for h.Len() > 0 {
		// 每次取出当前全局最小 key；若 key 相同，则由 heap 的 priority
		// 保证“较新的输入先出堆”。
		item := heap.Pop(h).(heapItem)

		if !hasLast || item.ent.Key != lastKey {
			// 只在遇到一个新 key 时输出一次，从而完成跨文件去重。
			if err := emit(item.ent); err != nil {
				return nil, nil, err
			}
			lastKey = item.ent.Key
			hasLast = true
		}

		// 推进当前迭代器，并把它的下一个候选项重新放回堆中，继续多路归并。
		if err := item.it.Next(); err != nil {
			cleanupCreated()
			return nil, nil, err
		}
		if item.it.Valid() {
			item.ent = item.it.Entry()
			heap.Push(h, item)
		}
	}

	// 刷出最后一个未满的分段。
	if err := flushChunk(); err != nil {
		return nil, nil, err
	}
	return outRuns, created, nil
}

func (d *DB) installCompactionLocked(res *compactionResult) error {
	job := res.job
	level := job.level

	oldv := d.currentVersion()
	if level < 0 || level+1 >= len(oldv.levels) {
		return nil
	}

	newv := oldv.withLevels()

	src := &newv.levels[level]
	dst := &newv.levels[level+1]

	// 1) 从源层移除 job.srcPaths。
	if level == 0 {
		src.l0Paths = removePaths(src.l0Paths, job.srcPaths)
	} else {
		src.runs = removeRunsByPaths(src.runs, job.srcPaths)
	}

	// 2) 重新算 overlap（用 job.minKey/maxKey，而不是用旧的 dstPaths 快照）。
	overIdx := overlappedRuns(dst.runs, job.minKey, job.maxKey)
	realOldDst := make([]string, 0, len(overIdx))
	for _, i := range overIdx {
		realOldDst = append(realOldDst, dst.runs[i].path)
	}

	// 3) 替换 dst 的 overlap 区间。
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

	// 4) 持久化 manifest。
	if err := d.persistManifestLevels(newv.levels); err != nil {
		return err
	}

	d.current = newv

	// 5) 删除旧文件：srcPaths + realOldDst。
	for _, p := range job.srcPaths {
		if d.cache != nil {
			d.cache.Evict(p)
		}
		if err := removeFileAndSync(p); err != nil {
			return err
		}
	}
	for _, p := range realOldDst {
		if d.cache != nil {
			d.cache.Evict(p)
		}
		if err := removeFileAndSync(p); err != nil {
			return err
		}
	}

	// 6) 清除 pending 标记。
	all := make([]string, 0, len(job.srcPaths)+len(realOldDst))
	all = append(all, job.srcPaths...)
	all = append(all, realOldDst...)
	d.clearPendingLocked(all)

	return nil
}

func (d *DB) pickCompactionLevelLocked() (int, bool) {
	v := d.currentVersion()

	type levelScore struct {
		level   int
		current int
		limit   int
		debt    int
	}

	betterThan := func(a, b levelScore) bool {
		// 先比较超阈值比例，再比较超额文件数，最后用层号做稳定 tie-break。
		left := a.current * b.limit
		right := b.current * a.limit
		if left != right {
			return left > right
		}
		if a.debt != b.debt {
			return a.debt > b.debt
		}
		return a.level < b.level
	}

	best := levelScore{level: -1}

	for level := 0; level < len(v.levels)-1; level++ {
		maxFiles := d.opt.levels[level].maxFiles
		if maxFiles <= 0 {
			continue
		}

		current := 0
		if level == 0 {
			current = len(v.levels[level].l0Paths)
		} else {
			current = len(v.levels[level].runs)
		}

		if current <= maxFiles {
			continue
		}

		debt := current - maxFiles

		cand := levelScore{
			level:   level,
			current: current,
			limit:   maxFiles,
			debt:    debt,
		}
		if best.level < 0 || betterThan(cand, best) {
			best = cand
		}
	}
	if best.level < 0 {
		return 0, false
	}
	return best.level, true
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
