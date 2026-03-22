package db

import (
	"container/heap"
	"sort"

	"monolithdb/internal/sstable"
	"monolithdb/internal/types"
)

// db_compaction.go 负责 compaction 的调度、执行和安装逻辑。

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

// RequestCompaction 主动请求一次后台 compaction。
func (d *DB) RequestCompaction() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.checkBGErrLocked(); err != nil {
		return err
	}
	d.requestCompactionLocked()
	return nil
}

// compactionLoop 在后台持续处理 compaction 请求。
func (d *DB) compactionLoop() {
	defer d.wg.Done()

	for {
		// 1) 等待请求或关闭信号。
		d.mu.Lock()
		for !d.closing && !d.compactionReq {
			d.cond.Wait()
		}
		if d.closing {
			d.mu.Unlock()
			return
		}

		d.compactionReq = false
		d.compacting = true
		d.mu.Unlock()

		// 2) 持续压实，直到没有任何层超阈值。
		for {
			d.mu.Lock()

			if d.closing {
				d.compacting = false
				d.mu.Unlock()
				return
			}

			if d.bgErr != nil {
				d.compacting = false
				d.mu.Unlock()
				break
			}

			level, ok := d.pickCompactionLevelLocked()
			if !ok {
				d.compacting = false
				d.mu.Unlock()
				break
			}
			job, err := d.pickCompactionJobLocked(level)
			d.mu.Unlock()

			if err != nil {
				d.mu.Lock()
				d.bgErr = err
				d.compacting = false
				d.mu.Unlock()
				break
			}
			if job == nil {
				d.mu.Lock()
				d.compacting = false
				d.mu.Unlock()
				break
			}

			res, err := d.doCompaction(job)
			if err != nil {
				d.mu.Lock()
				d.clearJobPendingLocked(job)
				d.bgErr = err
				d.compacting = false
				d.mu.Unlock()
				break
			}

			d.mu.Lock()
			if err := d.installCompactionLocked(res); err != nil {
				d.clearJobPendingLocked(job)
				d.bgErr = err
				d.compacting = false
				d.mu.Unlock()
				break
			}
			d.mu.Unlock()
		}
	}
}

// requestCompactionLocked 在锁内标记并唤醒后台 compaction。
func (d *DB) requestCompactionLocked() {
	d.compactionReq = true
	d.cond.Signal()
}

// checkBGErrLocked 返回后台协程记录的首个错误。
func (d *DB) checkBGErrLocked() error {
	if d.bgErr != nil {
		return d.bgErr
	}
	return nil
}

// pickCompactionLevelLocked 从超阈值的层中挑出最需要压实的一层。
func (d *DB) pickCompactionLevelLocked() (int, bool) {
	v := d.currentVersion()

	type levelScore struct {
		level   int
		current int
		limit   int
		debt    int
	}

	betterThan := func(a, b levelScore) bool {
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

// pickCompactionJobLocked 在指定层上挑选一份 compaction 任务。
func (d *DB) pickCompactionJobLocked(level int) (*compactionJob, error) {
	v := d.currentVersion()
	if level < 0 || level+1 >= len(v.levels) {
		return nil, nil
	}

	src := &v.levels[level]
	dst := &v.levels[level+1]

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

	minK, maxK, err := rangeOfFiles(pickedPaths)
	if err != nil {
		return nil, err
	}

	overIdx := overlappedRuns(dst.runs, minK, maxK)
	dstPaths := make([]string, 0, len(overIdx))
	for _, i := range overIdx {
		p := dst.runs[i].path
		if d.isPendingLocked(p) {
			return nil, nil
		}
		dstPaths = append(dstPaths, p)
	}

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

// doCompaction 执行一次 compaction，并生成新的 run 集合。
func (d *DB) doCompaction(job *compactionJob) (*compactionResult, error) {
	inputs := make([]string, 0, len(job.srcPaths)+len(job.dstPaths))
	inputs = append(inputs, job.srcPaths...)
	inputs = append(inputs, job.dstPaths...)

	dropBottomTombstones := job.level+1 == d.opt.numLevels-1
	newRuns, _, err := d.streamCompactionRuns(job.level+1, inputs, dropBottomTombstones)
	if err != nil {
		return nil, err
	}
	return &compactionResult{job: job, newRuns: newRuns}, nil
}

// streamCompactionRuns 以流式方式归并多个 SST 输入，并分段写出目标层 run。
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
		for _, p := range created {
			_ = removeFileAndSync(p)
		}
	}

	flushChunk := func() error {
		if len(chunk) == 0 {
			return nil
		}
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
		item := heap.Pop(h).(heapItem)

		if !hasLast || item.ent.Key != lastKey {
			if err := emit(item.ent); err != nil {
				return nil, nil, err
			}
			lastKey = item.ent.Key
			hasLast = true
		}

		if err := item.it.Next(); err != nil {
			cleanupCreated()
			return nil, nil, err
		}
		if item.it.Valid() {
			item.ent = item.it.Entry()
			heap.Push(h, item)
		}
	}

	if err := flushChunk(); err != nil {
		return nil, nil, err
	}
	return outRuns, created, nil
}

// installCompactionLocked 安装 compaction 结果，并删除被替换的旧文件。
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

	if level == 0 {
		src.l0Paths = removePaths(src.l0Paths, job.srcPaths)
	} else {
		src.runs = removeRunsByPaths(src.runs, job.srcPaths)
	}

	overIdx := overlappedRuns(dst.runs, job.minKey, job.maxKey)
	realOldDst := make([]string, 0, len(overIdx))
	for _, i := range overIdx {
		realOldDst = append(realOldDst, dst.runs[i].path)
	}

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

	if err := d.persistManifestLevels(newv.levels); err != nil {
		return err
	}

	d.current = newv

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

	all := make([]string, 0, len(job.srcPaths)+len(realOldDst))
	all = append(all, job.srcPaths...)
	all = append(all, realOldDst...)
	d.clearPendingLocked(all)

	return nil
}

// isPendingLocked 判断某个路径是否已经被后台任务占用。
func (d *DB) isPendingLocked(path string) bool {
	_, ok := d.pending[path]
	return ok
}

// markPendingLocked 标记一组路径正在被后台任务使用。
func (d *DB) markPendingLocked(paths []string) {
	for _, p := range paths {
		d.pending[p] = struct{}{}
	}
}

// clearPendingLocked 清理一组路径的 pending 标记。
func (d *DB) clearPendingLocked(paths []string) {
	for _, p := range paths {
		delete(d.pending, p)
	}
}

// clearJobPendingLocked 清理某个 compaction 任务占用的 pending 标记。
func (d *DB) clearJobPendingLocked(job *compactionJob) {
	if job == nil {
		return
	}
	all := make([]string, 0, len(job.srcPaths)+len(job.dstPaths))
	all = append(all, job.srcPaths...)
	all = append(all, job.dstPaths...)
	d.clearPendingLocked(all)
}
