package db

import (
	"fmt"
	"os"
	"path/filepath"

	"monolithdb/internal/memtable"
	"monolithdb/internal/sstable"
	"monolithdb/internal/types"
	"monolithdb/internal/wal"
)

type flushJob struct {
	entries []types.Entry
	id      uint64
	path    string
	tmpPath string
}

func (d *DB) prepareFlushLocked() (*flushJob, error) {
	if d.imm != nil {
		return nil, fmt.Errorf("db: flush already in progress")
	}

	if d.mem == nil || d.mem.ApproxSize() == 0 {
		return nil, nil
	}

	id := d.nextID
	v := d.currentVersion()
	path := filepath.Join(v.levels[0].dir, fmt.Sprintf("%06d.sst", id))
	tmpPath := path + ".tmp"

	// 1) freeze current memtable
	oldMem := d.mem
	d.imm = oldMem
	d.mem = memtable.NewMemTable()

	// 2) rotate wal:
	// forge.wal  -> forge.flush.wal
	// new forge.wal for new writes
	if err := d.wal.Close(); err != nil {
		// rollback
		d.mem = oldMem
		d.imm = nil
		return nil, err
	}

	_ = os.Remove(d.immWalPath) // 清理旧残留（正常情况下不应该存在）
	if err := os.Rename(d.walPath, d.immWalPath); err != nil {
		// 尝试恢复
		w, openErr := wal.Open(d.walPath)
		if openErr == nil {
			d.wal = w
		}
		d.mem = oldMem
		d.imm = nil
		return nil, err
	}

	newWal, err := wal.Open(d.walPath)
	if err != nil {
		// 尝试恢复：把 flush wal 改回去
		_ = os.Rename(d.immWalPath, d.walPath)
		w, openErr := wal.Open(d.walPath)
		if openErr == nil {
			d.wal = w
		}
		d.mem = oldMem
		d.imm = nil
		return nil, err
	}
	d.wal = newWal

	return &flushJob{
		id:      id,
		path:    path,
		tmpPath: tmpPath,
	}, nil
}

func (d *DB) doFlush(job *flushJob) error {
	if job == nil {
		return nil
	}
	if d.imm == nil {
		return fmt.Errorf("db: no immutable memtable to flush")
	}
	job.entries = d.imm.RangeAll("", "")
	if len(job.entries) == 0 {
		return fmt.Errorf("db: flush job has no entries")
	}

	if err := sstable.WriteTable(job.tmpPath, job.entries); err != nil {
		_ = os.Remove(job.tmpPath)
		return err
	}
	if err := os.Rename(job.tmpPath, job.path); err != nil {
		_ = os.Remove(job.tmpPath)
		return err
	}
	return nil
}

func (d *DB) installFlushLocked(job *flushJob) error {
	oldv := d.currentVersion()
	newv := oldv.withLevels()

	// 挂到 L0 头部
	newv.levels[0].l0Paths = append([]string{job.path}, newv.levels[0].l0Paths...)
	d.nextID++

	if err := d.persistManifestLevels(newv.levels); err != nil {
		return err
	}

	d.current = newv

	// flush 成功，冻结 mem / flush wal 可以清理掉
	d.imm = nil
	_ = os.Remove(d.immWalPath)

	// 唤醒后台 compaction
	d.requestCompactionLocked()
	return nil
}
