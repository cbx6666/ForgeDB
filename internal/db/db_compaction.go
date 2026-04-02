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

type compactionJob struct {
	// level 表示源层编号，目标层固定为 level+1。
	level int

	// srcPaths 是源层被选中的输入文件，dstPaths 是目标层和其范围重叠的旧文件。
	srcPaths []string
	dstPaths []string

	// minKey/maxKey 表示这轮 compaction 覆盖的整体 key 范围。
	minKey string
	maxKey string
}

type compactionResult struct {
	// job 保留原始任务元数据，便于安装阶段回收旧文件和清理 pending。
	job *compactionJob
	// newRuns 是这轮 compaction 产出的目标层新文件集合。
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
		for !d.closing && !d.compaction.compactionReq {
			d.compaction.cond.Wait()
		}
		if d.closing {
			d.mu.Unlock()
			return
		}

		d.compaction.compactionReq = false
		d.mu.Unlock()

		// 2) 持续压实，直到没有任何层超阈值。
		for {
			d.mu.Lock()

			if d.closing {
				d.mu.Unlock()
				return
			}

			if d.bgErr != nil {
				d.mu.Unlock()
				break
			}

			level, ok := d.pickCompactionLevelLocked()
			if !ok {
				// 当前已经没有层超阈值，这一轮后台 compaction 可以先收住。
				d.mu.Unlock()
				break
			}
			job, err := d.pickCompactionJobLocked(level)
			d.mu.Unlock()

			if err != nil {
				d.mu.Lock()
				d.bgErr = err
				// 一旦选任务出错，后续操作统一通过 bgErr 快速失败。
				d.mu.Unlock()
				break
			}
			if job == nil {
				break
			}

			res, err := d.doCompaction(job)
			if err != nil {
				d.mu.Lock()
				d.clearJobPendingLocked(job)
				// 执行阶段失败时必须清掉 pending，避免后续该范围永久不可选。
				d.bgErr = err
				d.mu.Unlock()
				break
			}

			d.mu.Lock()
			if err := d.installCompactionLocked(res); err != nil {
				d.clearJobPendingLocked(job)
				// 安装失败同样要先回收 pending，再把错误上升到全局后台错误。
				d.bgErr = err
				d.mu.Unlock()
				break
			}
			d.mu.Unlock()
		}
	}
}

// requestCompactionLocked 在锁内标记并唤醒后台 compaction。
func (d *DB) requestCompactionLocked() {
	d.compaction.compactionReq = true
	// compactionLoop 只有一个消费者，这里 signal 一个等待者即可。
	d.compaction.cond.Signal()
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

		// debt 表示当前层超出阈值多少个文件，用作同等比例下的次级排序键。
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
	// 返回最需要 compaction 的层，目标层固定为 best.level+1。
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

	// 2) 计算源层输入覆盖的整体 key 范围。
	minK, maxK, err := rangeOfFiles(pickedPaths)
	if err != nil {
		return nil, err
	}

	// 3) 找出目标层里和这段范围有交叠的文件。
	overIdx := overlappedRuns(dst.runs, minK, maxK)
	dstPaths := make([]string, 0, len(overIdx))
	for _, i := range overIdx {
		p := dst.runs[i].path
		if d.isPendingLocked(p) {
			// 目标层有重叠文件已被别的后台任务占用时，这轮任务直接放弃。
			return nil, nil
		}
		dstPaths = append(dstPaths, p)
	}

	// 4) 标记 pending，避免后台重复选中同一批文件。
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

	// 目标层已经是最后一层时，后面不再有更老数据需要被 tombstone 遮蔽，
	// 因此可以直接在这轮 compaction 中丢弃删除标记。
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
		// 为每个输入 SST 打开顺序迭代器，并记录输入优先级。
		// priority 越小，表示这一路输入越“新”，同 key 去重时应优先保留。
		it, err := sstable.NewIter(path)
		if err != nil {
			return nil, nil, err
		}
		iters = append(iters, it)
		if it.Valid() {
			// 只把当前有效项放进堆，后续靠迭代器推进维持多路归并。
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
		// chunk 的大小已经被 runMax 限住了，因此这里一次只写一个目标 run。
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
				return nil, nil, err
			}
			lastKey = item.ent.Key
			hasLast = true
		}

		// 推进当前输入迭代器，并把下一个候选项重新放回堆中。
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

// installCompactionLocked 安装 compaction 结果，并删除被替换的旧文件。
func (d *DB) installCompactionLocked(res *compactionResult) error {
	job := res.job
	level := job.level

	oldv := d.currentVersion()
	if level < 0 || level+1 >= len(oldv.levels) {
		// 越界结果直接忽略，避免在安装阶段把结构写坏。
		return nil
	}

	newv := oldv.withLevels()

	src := &newv.levels[level]
	dst := &newv.levels[level+1]

	// 1) 从源层移除当前 job 的输入文件。
	if level == 0 {
		src.l0Paths = removePaths(src.l0Paths, job.srcPaths)
	} else {
		src.runs = removeRunsByPaths(src.runs, job.srcPaths)
	}

	// 2) 重新计算目标层里真实重叠的旧文件区间。
	overIdx := overlappedRuns(dst.runs, job.minKey, job.maxKey)
	realOldDst := make([]string, 0, len(overIdx))
	for _, i := range overIdx {
		realOldDst = append(realOldDst, dst.runs[i].path)
	}

	// 3) 用新的 runs 替换目标层重叠区间。
	newDst := make([]levelFile, 0, len(dst.runs)-len(overIdx)+len(res.newRuns))
	if len(overIdx) == 0 {
		// 没有重叠时直接追加新 runs，再按 minKey 排序即可。
		newDst = append(newDst, dst.runs...)
		newDst = append(newDst, res.newRuns...)
		sort.Slice(newDst, func(i, j int) bool { return newDst[i].minKey < newDst[j].minKey })
	} else {
		// 有重叠时只替换那一段区间，其余 runs 保持原顺序。
		start := overIdx[0]
		end := overIdx[len(overIdx)-1] + 1
		newDst = append(newDst, dst.runs[:start]...)
		newDst = append(newDst, res.newRuns...)
		newDst = append(newDst, dst.runs[end:]...)
	}
	dst.runs = newDst

	// 4) manifest 持久化成功后，新版本视图才正式生效。
	if err := d.persistManifestLevels(newv.levels); err != nil {
		return err
	}

	d.current = newv

	// 5) 删除已经被新版本替换掉的旧文件。
	for _, p := range job.srcPaths {
		if d.cache != nil {
			// 删除文件前先驱逐缓存，避免后续命中已经失效的表对象。
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

	// 6) 清理 pending 标记，允许后续 compaction 再次选中相关范围。
	all := make([]string, 0, len(job.srcPaths)+len(realOldDst))
	all = append(all, job.srcPaths...)
	all = append(all, realOldDst...)
	d.clearPendingLocked(all)

	return nil
}

// isPendingLocked 判断某个路径是否已经被后台任务占用。
func (d *DB) isPendingLocked(path string) bool {
	_, ok := d.compaction.pendingPaths[path]
	return ok
}

// markPendingLocked 标记一组路径正在被后台任务使用。
func (d *DB) markPendingLocked(paths []string) {
	for _, p := range paths {
		// pending 按文件路径去重，避免后台任务重复消费同一输入。
		d.compaction.pendingPaths[p] = struct{}{}
	}
}

// clearPendingLocked 清理一组路径的 pending 标记。
func (d *DB) clearPendingLocked(paths []string) {
	for _, p := range paths {
		delete(d.compaction.pendingPaths, p)
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
