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

// db_flush.go 负责 flush 请求、后台刷盘、安装新 SST 以及自动 flush 逻辑。

// flushJob 表示一轮待执行的 flush 任务。
type flushJob struct {
	// entries 是最终要写入 SST 的有序记录，由 doFlush 在写盘前填充。
	entries []types.Entry
	// imm 是这轮 flush 绑定的冻结 memtable 及其 WAL 上下文。
	imm *immutableMem
	// id 是目标 SST 的文件号。
	id uint64
	// path 是最终 SST 路径，tmpPath 是写盘阶段使用的临时文件路径。
	path    string
	tmpPath string
}

// Flush 把当前可见的内存数据推进到 SST。
// 调用方只负责发起请求并等待至少推进到自己看到的最新代际。
func (d *DB) Flush() error {
	d.mu.Lock()
	if err := d.checkBGErrLocked(); err != nil {
		d.mu.Unlock()
		return err
	}
	if d.closing {
		d.mu.Unlock()
		return errDBClosed
	}

	// 显式 Flush 会先把当前 active memtable 冻结入队。
	if _, err := d.enqueueActiveMemtableLocked(); err != nil {
		d.mu.Unlock()
		return err
	}
	if !d.hasFlushWorkLocked() {
		d.mu.Unlock()
		return nil
	}

	target := d.flush.gen
	d.requestFlushLocked()
	err := d.waitFlushDoneLocked(target)
	d.mu.Unlock()
	return err
}

// autoFlushLoop 监听自动 flush 信号，并在达到阈值时把 active memtable 入队。
func (d *DB) autoFlushLoop() {
	defer d.wg.Done()

	for {
		select {
		case <-d.flush.autoCh:
		case <-d.flush.autoStop:
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

		if _, err := d.enqueueActiveMemtableLocked(); err != nil {
			if d.bgErr == nil {
				d.bgErr = err
			}
			d.flush.cond.Broadcast()
			d.mu.Unlock()
			return
		}
		d.requestFlushLocked()
		d.mu.Unlock()
	}
}

// flushLoop 串行消费 immutable queue，并把队头刷成新的 L0 SST。
func (d *DB) flushLoop() {
	defer d.wg.Done()

	for {
		d.mu.Lock()
		for !d.closing && !d.flush.req {
			d.flush.cond.Wait()
		}

		if d.bgErr != nil {
			d.flush.cond.Broadcast()
			d.mu.Unlock()
			return
		}
		if d.closing && !d.flush.req {
			d.flush.cond.Broadcast()
			d.mu.Unlock()
			return
		}

		// flushLoop 只消费已经入队的 immutable memtable，不负责主动冻结 active memtable。
		if d.flush.job == nil {
			d.flush.job = d.startFlushJobLocked()
		}

		job := d.flush.job
		d.flush.req = false
		if job == nil {
			d.flush.cond.Broadcast()
			d.mu.Unlock()
			continue
		}

		d.flush.running = true
		d.mu.Unlock()

		err := d.doFlush(job)

		d.mu.Lock()
		if err == nil {
			err = d.installFlushLocked()
		}
		if err != nil && d.bgErr == nil {
			d.bgErr = err
		}
		d.flush.running = false
		if err == nil && len(d.imms) > 0 {
			// 队列里还有后续 immutable memtable，继续推进下一轮。
			d.flush.req = true
		}
		d.flush.cond.Broadcast()
		shouldExit := d.bgErr != nil || (d.closing && !d.flush.req)
		d.mu.Unlock()

		if shouldExit {
			return
		}
	}
}

// doFlush 把一份 immutable memtable 写成 SST 文件。
func (d *DB) doFlush(job *flushJob) error {
	if job == nil {
		return nil
	}
	if job.imm == nil {
		return fmt.Errorf("db: no immutable memtable to flush")
	}
	if job.imm.mem == nil {
		return fmt.Errorf("db: immutable memtable missing")
	}

	job.entries = job.imm.mem.RangeAll("", "")
	if len(job.entries) == 0 {
		return fmt.Errorf("db: flush job has no entries")
	}

	// 先写临时文件，再 rename 成最终 SST，避免残留半成品。
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

// installFlushLocked 把当前队头 flush job 安装到 L0，并清理中间状态。
func (d *DB) installFlushLocked() error {
	job := d.flush.job
	if job == nil {
		return fmt.Errorf("db: no flush job to install")
	}
	if job.imm == nil || len(d.imms) == 0 || d.imms[0] != job.imm {
		return fmt.Errorf("db: flush state changed before install")
	}

	oldv := d.currentVersion()
	newv := oldv.withLevels()
	newv.levels[0].l0Paths = append([]string{job.path}, newv.levels[0].l0Paths...)

	if err := d.persistManifestLevels(newv.levels); err != nil {
		return err
	}

	d.current = newv
	d.imms = d.imms[1:]
	d.flush.job = nil
	d.flush.doneGen = job.imm.gen

	// 对应的 flush WAL 只有在 install 成功后才能删除。
	if err := removeFileAndSync(job.imm.walPath); err != nil {
		return err
	}

	d.requestCompactionLocked()
	return nil
}

// requestAutoFlush 触发一次异步自动 flush 检查。
func (d *DB) requestAutoFlush() {
	if d.flush.autoCh == nil {
		return
	}
	select {
	case d.flush.autoCh <- struct{}{}:
	default:
		// 通道已满说明后台已经有一次检查待处理，不需要重复排队。
	}
}

// requestFlushLocked 在锁内登记一次 flush 请求并唤醒后台 worker。
func (d *DB) requestFlushLocked() {
	d.flush.req = true
	if d.flush.cond != nil {
		d.flush.cond.Signal()
	}
}

// shouldAutoFlushLocked 判断当前 active memtable 是否达到自动 flush 阈值。
func (d *DB) shouldAutoFlushLocked() bool {
	return d.opt.autoFlushBytes > 0 &&
		d.mem != nil &&
		d.mem.ApproxSize() >= d.opt.autoFlushBytes
}

// flushQueueFullLocked 判断 immutable queue 是否已经达到上限。
func (d *DB) flushQueueFullLocked() bool {
	return d.opt.maxImmutableMems > 0 && len(d.imms) >= d.opt.maxImmutableMems
}

// hasFlushWorkLocked 判断当前是否仍有待刷数据或在途 flush 工作。
func (d *DB) hasFlushWorkLocked() bool {
	if len(d.imms) > 0 || d.flush.running || d.flush.job != nil {
		return true
	}
	return d.mem != nil && d.mem.ApproxSize() > 0
}

// waitFlushQueueRoomLocked 等待 immutable queue 出现空位。
func (d *DB) waitFlushQueueRoomLocked() error {
	for d.bgErr == nil && !d.closing && d.flushQueueFullLocked() {
		// 队列已满时，持续推动后台 worker 消化队头。
		d.requestFlushLocked()
		d.flush.cond.Wait()
	}
	if d.closing {
		return errDBClosed
	}
	return d.checkBGErrLocked()
}

// waitFlushDoneLocked 等待 flush 至少推进到目标代际。
func (d *DB) waitFlushDoneLocked(target uint64) error {
	for d.bgErr == nil && d.flush.doneGen < target {
		if !d.hasFlushWorkLocked() {
			break
		}
		d.flush.cond.Wait()
	}
	return d.checkBGErrLocked()
}

// enqueueActiveMemtableLocked 把当前 active memtable 冻结入队，并绑定独立 flush WAL。
func (d *DB) enqueueActiveMemtableLocked() (*immutableMem, error) {
	if d.mem == nil || d.mem.ApproxSize() == 0 {
		return nil, nil
	}
	if err := d.waitFlushQueueRoomLocked(); err != nil {
		return nil, err
	}

	immMem := d.freezeActiveMemLocked()
	gen := d.flush.gen + 1
	walPath := flushWALPath(d.dir, gen)
	if err := d.rotateFlushWALLocked(immMem, walPath); err != nil {
		return nil, err
	}

	imm := &immutableMem{
		gen:     gen,
		mem:     immMem,
		walPath: walPath,
	}
	d.imms = append(d.imms, imm)
	d.flush.gen = gen
	return imm, nil
}

// startFlushJobLocked 从当前队头 immutable memtable 构造 flush job。
func (d *DB) startFlushJobLocked() *flushJob {
	if d.flush.job != nil {
		return d.flush.job
	}
	if len(d.imms) == 0 {
		return nil
	}

	id := d.nextID
	// flush job 一旦创建，就立即预留对应的 SST 文件号，避免与 compaction 分配冲突。
	d.nextID++
	v := d.currentVersion()
	path := filepath.Join(v.levels[0].dir, fmt.Sprintf("%06d.sst", id))
	tmpPath := path + ".tmp"

	job := &flushJob{
		imm:     d.imms[0],
		id:      id,
		path:    path,
		tmpPath: tmpPath,
	}
	d.flush.job = job
	return job
}

// freezeActiveMemLocked 冻结当前 active memtable，并切换到新的 active memtable。
func (d *DB) freezeActiveMemLocked() *memtable.MemTable {
	imm := d.mem
	d.mem = memtable.NewMemTable()
	return imm
}

// rotateFlushWALLocked 把当前 active WAL 轮转成指定的 flush WAL。
func (d *DB) rotateFlushWALLocked(imm *memtable.MemTable, flushWALPath string) error {
	if err := d.wal.Close(); err != nil {
		d.mem = imm
		return err
	}

	_ = removeFileAndSync(flushWALPath)
	if err := os.Rename(d.walPath, flushWALPath); err != nil {
		w, openErr := wal.Open(d.walPath)
		if openErr == nil {
			d.wal = w
		}
		d.mem = imm
		return err
	}

	newWal, err := wal.Open(d.walPath)
	if err != nil {
		_ = os.Rename(flushWALPath, d.walPath)
		w, openErr := wal.Open(d.walPath)
		if openErr == nil {
			d.wal = w
		}
		d.mem = imm
		return err
	}
	d.wal = newWal

	if err := syncDirFn(d.dir); err != nil {
		// 目录同步失败时，把 prepare 前的 mem/WAL 布局尽量完整回滚。
		_ = d.wal.Close()
		d.wal = nil
		_ = os.Remove(d.walPath)
		_ = os.Rename(flushWALPath, d.walPath)

		restoredWAL, openErr := wal.Open(d.walPath)
		if openErr != nil {
			d.mem = imm
			return openErr
		}
		d.wal = restoredWAL
		d.mem = imm
		return err
	}
	return nil
}

// flushWALPath 返回 immutable memtable 对应的独立 flush WAL 路径。
func flushWALPath(dir string, gen uint64) string {
	return filepath.Join(dir, fmt.Sprintf("forge.flush.%020d.wal", gen))
}
