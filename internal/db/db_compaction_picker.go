package db

// db_compaction_picker.go 负责 compaction 的任务挑选和 pending 调度语义。

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

// dstLevel 返回该任务的目标层编号。
func (job *compactionJob) dstLevel() int {
	if job == nil {
		return -1
	}
	return job.level + 1
}

// allPaths 返回本轮 compaction 涉及的全部文件路径，顺序保持“源层在前、目标层在后”。
func (job *compactionJob) allPaths() []string {
	if job == nil {
		return nil
	}
	all := make([]string, 0, len(job.srcPaths)+len(job.dstPaths))
	all = append(all, job.srcPaths...)
	all = append(all, job.dstPaths...)
	return all
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
	pickedPaths := d.pickCompactionInputsLocked(level, src, pickN)
	if len(pickedPaths) == 0 {
		return nil, nil
	}

	// 2) 计算源层输入覆盖的整体 key 范围。
	minK, maxK, err := rangeOfFiles(pickedPaths)
	if err != nil {
		return nil, err
	}

	// 3) 找出目标层里和这段范围有交叠的文件。
	dstPaths, ok := d.pickCompactionOverlapLocked(dst, minK, maxK)
	if !ok {
		return nil, nil
	}

	job := &compactionJob{
		level:    level,
		srcPaths: append([]string(nil), pickedPaths...),
		dstPaths: append([]string(nil), dstPaths...),
		minKey:   minK,
		maxKey:   maxK,
	}

	// 4) 标记 pending，避免后台重复选中同一批文件。
	d.markPendingLocked(job.allPaths())

	return job, nil
}

// pickCompactionInputsLocked 从源层挑出一批 compaction 输入文件。
func (d *DB) pickCompactionInputsLocked(level int, src *level, pickN int) []string {
	if src == nil || pickN <= 0 {
		return nil
	}

	if level == 0 {
		picked, _ := pickOldestL0(src.l0Paths, pickN, d.isPendingLocked)
		return picked
	}

	pickedRuns, _ := pickOldestRuns(src.runs, pickN, d.isPendingLocked)
	paths := make([]string, 0, len(pickedRuns))
	for _, f := range pickedRuns {
		paths = append(paths, f.path)
	}
	return paths
}

// pickCompactionOverlapLocked 找出目标层里与给定范围重叠且未被占用的文件。
func (d *DB) pickCompactionOverlapLocked(dst *level, minK, maxK string) ([]string, bool) {
	if dst == nil {
		return nil, true
	}

	overIdx := overlappedRuns(dst.runs, minK, maxK)
	dstPaths := make([]string, 0, len(overIdx))
	for _, i := range overIdx {
		p := dst.runs[i].path
		if d.isPendingLocked(p) {
			// 目标层有重叠文件已被别的后台任务占用时，这轮任务直接放弃。
			return nil, false
		}
		dstPaths = append(dstPaths, p)
	}
	return dstPaths, true
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
