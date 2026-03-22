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

// db_flush.go 负责 flush 状态转换、落盘安装和自动 flush 逻辑。

type flushJob struct {
	entries []types.Entry
	id      uint64
	path    string
	tmpPath string
}

// db_flush.go 收拢 flush 相关的前台入口、后台循环和状态转换。

// Flush 将当前 memtable 刷成新的 L0 SST。
func (d *DB) Flush() error {
	d.mu.Lock()
	if err := d.checkBGErrLocked(); err != nil {
		d.mu.Unlock()
		return err
	}

	job, err := d.prepareFlushLocked()
	d.mu.Unlock()
	if err != nil {
		return err
	}
	if job == nil {
		return nil
	}

	// 真正的磁盘 IO 在锁外执行。
	if err := d.doFlush(job); err != nil {
		d.mu.Lock()
		d.bgErr = err
		d.mu.Unlock()
		return err
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if err := d.installFlushLocked(job); err != nil {
		d.bgErr = err
		return err
	}
	return nil
}

// requestAutoFlush 触发一次异步自动 flush 检查。
func (d *DB) requestAutoFlush() {
	if d.autoFlushCh == nil {
		return
	}
	select {
	case d.autoFlushCh <- struct{}{}:
	default:
	}
}

// shouldAutoFlushLocked 判断当前 memtable 是否已经达到自动 flush 阈值。
func (d *DB) shouldAutoFlushLocked() bool {
	return d.opt.autoFlushBytes > 0 &&
		d.imm == nil &&
		d.mem != nil &&
		d.mem.ApproxSize() >= d.opt.autoFlushBytes
}

// autoFlushLoop 在后台监听自动 flush 触发信号。
func (d *DB) autoFlushLoop() {
	defer d.wg.Done()

	for {
		select {
		case <-d.autoFlushCh:
		case <-d.autoFlushStop:
			return
		}

		d.mu.Lock()
		if d.closing {
			d.mu.Unlock()
			return
		}
		if err := d.checkBGErrLocked(); err != nil {
			d.mu.Unlock()
			return
		}
		if !d.shouldAutoFlushLocked() {
			d.mu.Unlock()
			continue
		}

		job, err := d.prepareFlushLocked()
		d.mu.Unlock()
		if err != nil {
			d.mu.Lock()
			d.bgErr = err
			d.mu.Unlock()
			return
		}
		if job == nil {
			continue
		}

		if err := d.doFlush(job); err != nil {
			d.mu.Lock()
			d.bgErr = err
			d.mu.Unlock()
			return
		}

		d.mu.Lock()
		if err := d.installFlushLocked(job); err != nil {
			d.bgErr = err
			d.mu.Unlock()
			return
		}

		// 如果前台写入期间又超了阈值，继续触发下一轮。
		if d.shouldAutoFlushLocked() {
			d.requestAutoFlush()
		}
		d.mu.Unlock()
	}
}

// prepareFlushLocked 冻结当前 memtable，切 WAL，并返回 flush 任务。
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

	// 1) 冻结当前 memtable。
	oldMem := d.mem
	d.imm = oldMem
	d.mem = memtable.NewMemTable()

	// 2) 轮转 WAL：
	// forge.wal -> forge.flush.wal
	// 然后重新打开新的 forge.wal 供后续写入使用。
	if err := d.wal.Close(); err != nil {
		d.mem = oldMem
		d.imm = nil
		return nil, err
	}

	// 清理旧残留，正常情况下这里不应存在旧文件。
	_ = removeFileAndSync(d.immWalPath)
	if err := os.Rename(d.walPath, d.immWalPath); err != nil {
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

	if err := syncDirFn(d.dir); err != nil {
		return nil, err
	}

	return &flushJob{
		id:      id,
		path:    path,
		tmpPath: tmpPath,
	}, nil
}

// doFlush 把 immutable memtable 写成 SST 文件。
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
	if err := syncPathDir(job.path); err != nil {
		return err
	}
	return nil
}

// installFlushLocked 把新的 SST 安装到 L0，并清理 flush 中间态。
func (d *DB) installFlushLocked(job *flushJob) error {
	oldv := d.currentVersion()
	newv := oldv.withLevels()

	// 挂到 L0 头部。
	newv.levels[0].l0Paths = append([]string{job.path}, newv.levels[0].l0Paths...)
	d.nextID++

	if err := d.persistManifestLevels(newv.levels); err != nil {
		return err
	}

	d.current = newv

	// flush 成功后，冻结 memtable 和 flush WAL 都可以清理掉。
	d.imm = nil
	if err := removeFileAndSync(d.immWalPath); err != nil {
		return err
	}

	// 唤醒后台 compaction。
	d.requestCompactionLocked()
	return nil
}
