package db

import "sort"

// db_compaction_install.go 负责 compaction 结果安装、旧文件回收和失败清理。

// installCompactionLocked 安装 compaction 结果，并删除被替换的旧文件。
func (d *DB) installCompactionLocked(job *compactionJob, newRuns []levelFile) error {
	level := job.level
	dstLevel := job.dstLevel()

	oldv := d.currentVersion()
	if level < 0 || dstLevel < 0 || dstLevel >= len(oldv.levels) {
		// 越界结果直接忽略，避免在安装阶段把结构写坏。
		return nil
	}

	newv := oldv.withLevels()

	src := &newv.levels[level]
	dst := &newv.levels[dstLevel]

	// 1) 从源层移除当前 job 的输入文件。
	if level == 0 {
		src.l0Paths = removePaths(src.l0Paths, job.srcPaths)
	} else {
		src.runs = removeRunsByPaths(src.runs, job.srcPaths)
	}

	// 2) 重新计算目标层里真实重叠的旧文件区间。
	overIdx, realOldDst := compactionInstallOverlap(dst.runs, job.minKey, job.maxKey)

	// 3) 用新的 runs 替换目标层重叠区间。
	dst.runs = replaceCompactionDstRuns(dst.runs, overIdx, newRuns)

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
	d.clearPendingLocked(job.allPaths())

	return nil
}

// compactionInstallOverlap 返回安装阶段真实重叠的目标层文件索引及其路径。
func compactionInstallOverlap(runs []levelFile, minK, maxK string) ([]int, []string) {
	overIdx := overlappedRuns(runs, minK, maxK)
	realOldDst := make([]string, 0, len(overIdx))
	for _, i := range overIdx {
		realOldDst = append(realOldDst, runs[i].path)
	}
	return overIdx, realOldDst
}

// replaceCompactionDstRuns 用 compaction 新输出替换目标层中与任务范围重叠的那段区间。
func replaceCompactionDstRuns(oldRuns []levelFile, overIdx []int, newRuns []levelFile) []levelFile {
	newDst := make([]levelFile, 0, len(oldRuns)-len(overIdx)+len(newRuns))
	if len(overIdx) == 0 {
		// 没有重叠时直接追加新 runs，再按 minKey 排序即可。
		newDst = append(newDst, oldRuns...)
		newDst = append(newDst, newRuns...)
		sort.Slice(newDst, func(i, j int) bool { return newDst[i].minKey < newDst[j].minKey })
		return newDst
	}

	// 有重叠时只替换那一段区间，其余 runs 保持原顺序。
	start := overIdx[0]
	end := overIdx[len(overIdx)-1] + 1
	newDst = append(newDst, oldRuns[:start]...)
	newDst = append(newDst, newRuns...)
	newDst = append(newDst, oldRuns[end:]...)
	return newDst
}

// failCompactionLocked 在锁内统一处理 compaction 失败路径。
func (d *DB) failCompactionLocked(job *compactionJob, err error) {
	if job != nil {
		d.clearPendingLocked(job.allPaths())
	}
	if err != nil && d.bgErr == nil {
		d.bgErr = err
	}
}
