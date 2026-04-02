package db

import (
	"errors"
	"time"

	"monolithdb/internal/memtable"
	"monolithdb/internal/wal"
)

// db_write.go 负责公开写接口、写请求提交、后台 group commit 和 WAL 同步逻辑。

const (
	writeBatchMaxCount = 128
	writeBatchMaxDelay = time.Millisecond
)

type writeOp byte

const (
	writeOpPut writeOp = iota
	writeOpDelete
)

type batchOp struct {
	op    writeOp
	key   string
	value []byte
}

type writeReq struct {
	ops  []batchOp
	done chan error
}

// errDBClosed 表示数据库已经关闭，不能再接收新的写请求。
var errDBClosed = errors.New("db: closed")

// WriteBatch 用于收集一组要一起提交的变更。
// 零值可直接使用。
type WriteBatch struct {
	ops []batchOp
}

// Put 向批量写中追加一条写入操作。
func (b *WriteBatch) Put(key string, value []byte) {
	if b == nil {
		return
	}
	b.ops = append(b.ops, batchOp{
		op:    writeOpPut,
		key:   key,
		value: cloneWriteValue(value),
	})
}

// Delete 向批量写中追加一条删除操作。
func (b *WriteBatch) Delete(key string) {
	if b == nil {
		return
	}
	b.ops = append(b.ops, batchOp{
		op:  writeOpDelete,
		key: key,
	})
}

// Len 返回当前批量写中的操作数。
func (b *WriteBatch) Len() int {
	if b == nil {
		return 0
	}
	return len(b.ops)
}

// Put 提交单条写入请求。
func (d *DB) Put(key string, value []byte) error {
	return d.submitWrite(writeOpPut, key, value)
}

// Delete 提交单条删除请求。
func (d *DB) Delete(key string) error {
	return d.submitWrite(writeOpDelete, key, nil)
}

// Write 提交一组 Put/Delete 组成的批量写请求。
func (d *DB) Write(batch *WriteBatch) error {
	if batch == nil || len(batch.ops) == 0 {
		return nil
	}
	return d.submitWriteBatch(batch.ops)
}

// submitWrite 把单条写请求包装成批量接口，复用统一的提交流程。
func (d *DB) submitWrite(op writeOp, key string, value []byte) error {
	return d.submitWriteBatch([]batchOp{{
		op:    op,
		key:   key,
		value: cloneWriteValue(value),
	}})
}

// submitWriteBatch 把一批写操作投递到后台 writeLoop。
func (d *DB) submitWriteBatch(ops []batchOp) error {
	if len(ops) == 0 {
		return nil
	}

	req := &writeReq{
		ops:  cloneBatchOps(ops),
		done: make(chan error, 1),
	}

	// 写入口和 Close 会竞争写通道，因此这里先检查关闭状态，再把请求交给后台。
	d.write.mu.RLock()
	if d.write.closed {
		d.write.mu.RUnlock()
		return errDBClosed
	}
	d.write.ch <- req
	d.write.mu.RUnlock()

	return <-req.done
}

// writeLoop 在后台聚合前台写请求，并执行 group commit。
func (d *DB) writeLoop() {
	defer d.wg.Done()

	for {
		first, ok := <-d.write.ch
		if !ok {
			return
		}

		batch, closed := d.collectWriteBatch(first)
		d.applyWriteBatch(batch)
		if closed {
			return
		}
	}
}

// collectWriteBatch 以短时间窗口继续吸收后续请求，形成一轮 group commit。
func (d *DB) collectWriteBatch(first *writeReq) ([]*writeReq, bool) {
	batch := []*writeReq{first}
	timer := time.NewTimer(writeBatchMaxDelay)
	closed := false

collect:
	for len(batch) < writeBatchMaxCount {
		select {
		case req, ok := <-d.write.ch:
			if !ok {
				closed = true
				break collect
			}
			batch = append(batch, req)
		case <-timer.C:
			break collect
		}
	}

	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}

	return batch, closed
}

// applyWriteBatch 在 WAL 和 MemTable 上原子地应用一批写请求。
func (d *DB) applyWriteBatch(batch []*writeReq) {
	d.mu.Lock()

	err := d.checkBGErrLocked()
	if err == nil {
		// 1) 先把这一轮 group commit 追加到 WAL，并按配置决定是否立即 fsync。
		err = d.appendWriteBatchWALLocked(batch)

		if err == nil {
			// 2) WAL 成功后，按原顺序把写入应用到 active memtable。
			applyBatchToMemtable(d.mem, batch)
			// 3) 如果 active mem 超过自动 flush 阈值，则请求后台 flush，并在队列满时施加背压。
			err = d.waitWriteFlushBackpressureLocked()
		} else {
			d.bgErr = err
		}
	}

	d.mu.Unlock()

	for _, req := range batch {
		req.done <- err
	}
}

// appendWriteBatchWALLocked 先把一轮 group commit 写入 WAL，再按策略决定是否立即 fsync。
func (d *DB) appendWriteBatchWALLocked(batch []*writeReq) error {
	records := buildWALRecords(batch)

	// 保持 WAL-before-mem 语义，避免内存数据已经可见但 WAL 尚未落盘。
	if err := d.wal.AppendBatch(records); err != nil {
		return err
	}

	d.write.seq++

	if d.opt.walSync != WALSyncEveryBatch {
		return nil
	}

	if err := d.write.syncWAL(); err != nil {
		return err
	}
	d.write.syncedSeq = d.write.seq
	return nil
}

// waitWriteFlushBackpressureLocked 在写入触发自动 flush 后处理队列背压。
func (d *DB) waitWriteFlushBackpressureLocked() error {
	if !d.shouldAutoFlushLocked() {
		return nil
	}

	d.requestAutoFlush()

	for d.bgErr == nil && !d.closing && d.shouldAutoFlushLocked() && d.flushQueueFullLocked() {
		// immutable queue 已满且 active mem 继续超过阈值时，写入方要等待后台 flush 腾出空位，
		// 否则 active mem 会持续膨胀，背压就失效了。
		d.requestFlushLocked()
		d.flush.cond.Wait()
	}

	if d.bgErr != nil {
		return d.bgErr
	}
	return nil
}

// walSyncLoop 按固定间隔同步 WAL。
func (d *DB) walSyncLoop() {
	defer d.wg.Done()

	ticker := time.NewTicker(d.opt.walSyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := d.syncPendingWAL(); err != nil {
				return
			}
		case <-d.write.syncStop:
			return
		}
	}
}

// syncPendingWAL 把尚未 fsync 的 WAL 内容落盘。
func (d *DB) syncPendingWAL() error {
	d.mu.Lock()
	if d.bgErr != nil {
		err := d.bgErr
		d.mu.Unlock()
		return err
	}
	if d.write.seq == d.write.syncedSeq {
		d.mu.Unlock()
		return nil
	}

	// 先截取当前待同步边界，再锁外执行 fsync，避免长时间阻塞前台。
	targetSeq := d.write.seq
	d.mu.Unlock()

	if err := d.write.syncWAL(); err != nil {
		d.mu.Lock()
		if d.bgErr == nil {
			d.bgErr = err
		}
		d.mu.Unlock()
		return err
	}

	d.mu.Lock()
	if d.write.syncedSeq < targetSeq {
		d.write.syncedSeq = targetSeq
	}
	d.mu.Unlock()
	return nil
}

// cloneBatchOps 深拷贝一组写操作，避免调用方后续修改底层切片。
func cloneBatchOps(ops []batchOp) []batchOp {
	cloned := make([]batchOp, len(ops))
	for i, op := range ops {
		cloned[i] = batchOp{
			op:    op.op,
			key:   op.key,
			value: cloneWriteValue(op.value),
		}
	}
	return cloned
}

// buildWALRecords 把一轮 group commit 批次转换成 WAL 记录。
func buildWALRecords(batch []*writeReq) []wal.Record {
	totalOps := 0
	for _, req := range batch {
		totalOps += len(req.ops)
	}

	records := make([]wal.Record, 0, totalOps)
	for _, req := range batch {
		for _, op := range req.ops {
			rec := wal.Record{
				Key:   op.key,
				Value: op.value,
			}
			if op.op == writeOpDelete {
				rec.Op = 1
			}
			records = append(records, rec)
		}
	}
	return records
}

// applyBatchToMemtable 把一轮 group commit 中的操作按原顺序写入 active memtable。
func applyBatchToMemtable(mem *memtable.MemTable, batch []*writeReq) {
	for _, req := range batch {
		for _, op := range req.ops {
			if op.op == writeOpDelete {
				mem.Delete(op.key)
			} else {
				mem.Put(op.key, op.value)
			}
		}
	}
}

// cloneWriteValue 复制写入值，避免调用方后续修改底层切片。
func cloneWriteValue(value []byte) []byte {
	if value == nil {
		return nil
	}
	cp := make([]byte, len(value))
	copy(cp, value)
	return cp
}
