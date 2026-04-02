package db

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"monolithdb/internal/manifest"
	"monolithdb/internal/memtable"
	"monolithdb/internal/sstable"
	"monolithdb/internal/wal"
)

// db_open_close.go 负责数据库的打开、关闭和 MANIFEST 持久化。

// Open 使用默认选项打开数据库。
func Open(dir string) (*DB, error) {
	return OpenWithOptions(dir, defaultOptions())
}

// OpenWithOptions 初始化目录、恢复状态并启动后台协程。
func OpenWithOptions(dir string, opt Options) (*DB, error) {
	if err := validateOptions(opt); err != nil {
		return nil, err
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	sstDir := filepath.Join(dir, "sst")
	if err := os.MkdirAll(sstDir, 0o755); err != nil {
		return nil, err
	}

	manPath := filepath.Join(dir, "MANIFEST")
	man, err := manifest.Open(manPath)
	if err != nil {
		return nil, err
	}

	walPath := filepath.Join(dir, "forge.wal")
	w, err := wal.Open(walPath)
	if err != nil {
		_ = man.Close()
		return nil, err
	}

	m := memtable.NewMemTable()

	// 回放 WAL，把历史操作重新应用到内存表。
	replayInto := func(path string, m *memtable.MemTable) error {
		records, err := wal.Replay(path)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		for _, r := range records {
			switch r.Op {
			case 0:
				m.Put(r.Key, r.Value)
			case 1:
				m.Delete(r.Key)
			default:
				return wal.ErrCorruptWAL
			}
		}
		return nil
	}

	flushWALs, err := listFlushWALPaths(dir)
	if err != nil {
		_ = w.Close()
		_ = man.Close()
		return nil, err
	}
	for _, flushWALPath := range flushWALs {
		if err := replayInto(flushWALPath, m); err != nil {
			_ = w.Close()
			_ = man.Close()
			return nil, err
		}
	}
	if err := replayInto(walPath, m); err != nil {
		_ = w.Close()
		_ = man.Close()
		return nil, err
	}

	var levels []level
	var nextID uint64

	levels, nextID, err = recoverFromManifest(manPath, sstDir, opt)
	if err != nil {
		// 只允许两类错误回退到目录扫描恢复。
		if errors.Is(err, manifest.ErrNoSnapshot) || os.IsNotExist(err) {
			levels, nextID, err = scanAllLevels(sstDir, opt)
			if err != nil {
				_ = w.Close()
				_ = man.Close()
				return nil, err
			}

			// 首次从目录扫描恢复后，立即把结果写回 MANIFEST。
			if err := man.WriteSnapshot(nextID, levelsToSnapshot(levels)); err != nil {
				_ = w.Close()
				_ = man.Close()
				return nil, err
			}
		} else {
			_ = w.Close()
			_ = man.Close()
			return nil, err
		}
	}

	orphan, err := sanityCheckSSTDir(sstDir, levels)
	if err != nil {
		_ = w.Close()
		_ = man.Close()
		return nil, err
	}
	if len(orphan) > 0 {
		show := orphan
		if len(show) > 10 {
			show = show[:10]
		}
		fmt.Printf("WARN: db: removed orphan sst files not referenced by manifest: %v (total=%d)\n", show, len(orphan))
	}

	db := &DB{
		mem:        m,
		wal:        w,
		man:        man,
		dir:        dir,
		walPath:    walPath,
		sstDir:     sstDir,
		current:    newVersionFromLevels(levels),
		opt:        opt,
		nextID:     nextID,
		cache:      sstable.NewTableCache(),
		write: writeState{
			ch:      make(chan *writeReq, 256),
			syncWAL: w.Sync,
		},
		compaction: compactionState{
			pendingPaths: make(map[string]struct{}),
		},
	}

	if opt.walSync == WALSyncInterval {
		db.write.syncStop = make(chan struct{})
	}

	if opt.autoFlushBytes > 0 {
		db.flush.autoCh = make(chan struct{}, 1)
		db.flush.autoStop = make(chan struct{})
	}

	db.compaction.cond = sync.NewCond(&db.mu)
	db.flush.cond = sync.NewCond(&db.mu)

	// 启动后台协程。
	db.wg.Add(1)
	go db.flushLoop()

	db.wg.Add(1)
	go db.compactionLoop()

	db.wg.Add(1)
	go db.writeLoop()

	if db.write.syncStop != nil {
		db.wg.Add(1)
		go db.walSyncLoop()
	}

	if db.flush.autoCh != nil {
		db.wg.Add(1)
		go db.autoFlushLoop()
		// 启动后如果内存表已超阈值，立刻触发一次检查。
		db.requestAutoFlush()
	}

	return db, nil
}

// listFlushWALPaths 返回目录中所有需要在启动时回放的 flush WAL，按旧到新排序。
func listFlushWALPaths(dir string) ([]string, error) {
	pattern := filepath.Join(dir, "forge.flush.*.wal")

	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}
	sort.Strings(matches)
	return matches, nil
}

// Close 停止后台协程并关闭底层资源。
func (d *DB) Close() error {
	d.mu.Lock()
	d.closing = true
	if d.compaction.cond != nil {
		// 唤醒所有等待中的后台协程。
		d.compaction.cond.Broadcast()
	}
	if d.flush.cond != nil {
		// 唤醒所有等待中的 flush 协程和调用方。
		d.flush.cond.Broadcast()
	}
	d.mu.Unlock()

	d.write.mu.Lock()
	if !d.write.closed {
		d.write.closed = true
		close(d.write.ch)
	}
	d.write.mu.Unlock()

	if d.write.syncStop != nil {
		close(d.write.syncStop)
	}
	if d.flush.autoStop != nil {
		close(d.flush.autoStop)
	}

	// 等待后台协程退出后再关闭文件资源。
	d.wg.Wait()

	var syncErr error
	if d.opt.walSync == WALSyncInterval {
		syncErr = d.syncPendingWAL()
	}

	var err1, err2, err3 error
	if d.man != nil {
		err1 = d.man.Close()
	}
	if d.wal != nil {
		err2 = d.wal.Close()
	}
	if d.cache != nil {
		err3 = d.cache.Close()
	}
	if err1 != nil {
		return err1
	}
	if syncErr != nil {
		return syncErr
	}
	if err2 != nil {
		return err2
	}
	return err3
}

// persistManifestLevels 将层级快照持久化到 MANIFEST。
func (d *DB) persistManifestLevels(levels []level) error {
	if d.man == nil {
		return nil
	}

	// 先追加当前快照。
	if err := d.man.WriteSnapshot(d.nextID, levelsToSnapshot(levels)); err != nil {
		return err
	}

	// 达到阈值后执行一次 checkpoint。
	d.manifestWrites++

	const checkpointEvery = 50
	if d.manifestWrites >= checkpointEvery {
		if err := d.man.CheckpointLatest(); err != nil {
			return err
		}
		d.manifestWrites = 0
	}

	return nil
}
