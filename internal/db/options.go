package db

import (
	"fmt"
	"time"
)

type LevelOptions struct {
	// 触发 compaction 的阈值
	maxFiles int

	// 每次 compaction 从该层挑选的文件数
	pickN int

	// 目标层输出 run 的最大 entry 数（split）。
	// 对 L0 无意义，填 0 即可。
	runMaxEntries int
}

type WALSyncMode int

const (
	// WALSyncNever 只 Flush，不做 fsync。
	WALSyncNever WALSyncMode = iota
	// WALSyncEveryBatch 每个批次写完都执行一次 fsync。
	WALSyncEveryBatch
	// WALSyncInterval 由后台协程按固定间隔执行 fsync。
	WALSyncInterval
)

type Options struct {
	numLevels       int
	levels          []LevelOptions
	walSync         WALSyncMode
	walSyncInterval time.Duration
}

// DefaultOptions 返回一份可供调用方修改的默认配置副本。
func DefaultOptions() Options {
	return defaultOptions()
}

// WithWALSync 返回一份带有 WAL sync 策略的新配置。
// 对 WALSyncInterval，interval 必须大于 0；其他模式下该值会被忽略。
func (opt Options) WithWALSync(mode WALSyncMode, interval time.Duration) Options {
	opt.walSync = mode
	if mode == WALSyncInterval && interval > 0 {
		opt.walSyncInterval = interval
	}
	return opt
}

func defaultOptions() Options {
	return Options{
		numLevels: 3,
		levels: []LevelOptions{
			{maxFiles: 4, pickN: 2, runMaxEntries: 0},
			{maxFiles: 8, pickN: 1, runMaxEntries: 256},
			{maxFiles: 0, pickN: 0, runMaxEntries: 1024},
		},
		walSync:         WALSyncNever,
		walSyncInterval: 10 * time.Millisecond,
	}
}

func validateOptions(opt Options) error {
	if opt.numLevels < 2 {
		return fmt.Errorf("db: numLevels must be >= 2")
	}
	if len(opt.levels) != opt.numLevels {
		return fmt.Errorf("db: levels len(%d) != numLevels(%d)", len(opt.levels), opt.numLevels)
	}

	switch opt.walSync {
	case WALSyncNever, WALSyncEveryBatch:
	case WALSyncInterval:
		if opt.walSyncInterval <= 0 {
			return fmt.Errorf("db: walSyncInterval must be > 0 when walSync=Interval")
		}
	default:
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
