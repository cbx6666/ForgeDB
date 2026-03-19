package db

import "fmt"

type LevelOptions struct {
	// 触发 compaction 的阈值
	maxFiles int

	// 每次 compaction 从该层挑选的文件数
	pickN int

	// 目标层输出 run 的最大 entry 数（split）。对 L0 无意义，填 0 即可。
	runMaxEntries int
}

type WALSyncMode int

const (
	// WALSyncNever 只 Flush，不做 fsync。
	WALSyncNever WALSyncMode = iota
	// WALSyncEveryBatch 每个批次写完都执行一次 fsync。
	WALSyncEveryBatch
)

type Options struct {
	numLevels int
	levels    []LevelOptions
	walSync   WALSyncMode
}

func defaultOptions() Options {
	return Options{
		numLevels: 3,
		levels: []LevelOptions{
			{maxFiles: 4, pickN: 2, runMaxEntries: 0},
			{maxFiles: 8, pickN: 1, runMaxEntries: 256},
			{maxFiles: 0, pickN: 0, runMaxEntries: 1024},
		},
		walSync: WALSyncNever,
	}
}

func validateOptions(opt Options) error {
	if opt.numLevels < 2 {
		return fmt.Errorf("db: numLevels must be >= 2")
	}
	if len(opt.levels) != opt.numLevels {
		return fmt.Errorf("db: levels len(%d) != numLevels(%d)", len(opt.levels), opt.numLevels)
	}
	if opt.walSync != WALSyncNever && opt.walSync != WALSyncEveryBatch {
		return fmt.Errorf("db: invalid walSync mode %d", opt.walSync)
	}

	for i := 0; i < opt.numLevels; i++ {
		lv := opt.levels[i]

		if i < opt.numLevels-1 {
			if lv.maxFiles < 0 {
				return fmt.Errorf("db: level %d maxFiles < 0", i)
			}
			if lv.maxFiles > 0 && lv.pickN <= 0 {
				return fmt.Errorf("db: level %d has maxFiles=%d but pickN=%d (must be >0)",
					i, lv.maxFiles, lv.pickN)
			}
		}

		// L>=1 必须能 split run
		if i >= 1 {
			if lv.runMaxEntries <= 0 {
				return fmt.Errorf("db: level %d runMaxEntries must be >0", i)
			}
		} else {
			// L0 runMaxEntries 应为 0（只是提醒，不强制也可以）
			if lv.runMaxEntries != 0 {
				return fmt.Errorf("db: level 0 runMaxEntries must be 0")
			}
		}
	}

	return nil
}
