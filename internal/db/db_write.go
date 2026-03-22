package db

import (
	"errors"
	"time"

	"monolithdb/internal/wal"
)

// db_write.go 负责前台写入接口、写入调度和 WAL 同步逻辑。

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

// WriteBatch 用于收集多条要一起提交的变更。
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

// Put 提交单条写请求。
func (d *DB) Put(key string, value []byte) error {
	return d.submitWrite(writeOpPut, key, value)
}

// Write 提交一组 Put/Delete 组成的批量写请求。
func (d *DB) Write(batch *WriteBatch) error {
	if batch == nil || len(batch.ops) == 0 {
		return nil
	}
	return d.submitWriteBatch(batch.ops)
}

// Delete 提交单条删除请求。
func (d *DB) Delete(key string) error {
	return d.submitWrite(writeOpDelete, key, nil)
}

// submitWrite 把单条写请求包装成批量接口，复用统一提交流程。
func (d *DB) submitWrite(op writeOp, key string, value []byte) error {
	return d.submitWriteBatch([]batchOp{{
		op:    op,
		key:   key,
		value: cloneWriteValue(value),
	}})
}

// submitWriteBatch 把一批写操作投递到后台写协程。
func (d *DB) submitWriteBatch(ops []batchOp) error {
	if len(ops) == 0 {
		return nil
	}

	cloned := make([]batchOp, len(ops))
	for i, op := range ops {
		cloned[i] = batchOp{
			op:    op.op,
			key:   op.key,
			value: cloneWriteValue(op.value),
		}
	}

	req := &writeReq{
		ops:  cloned,
		done: make(chan error, 1),
	}

	d.writeMu.RLock()
	if d.writeClosed {
		d.writeMu.RUnlock()
		return errDBClosed
	}
	d.writeCh <- req
	d.writeMu.RUnlock()

	return <-req.done
}

// writeLoop 在后台聚合前台写请求并执行 group commit。
func (d *DB) writeLoop() {
	defer d.wg.Done()

	for {
		first, ok := <-d.writeCh
		if !ok {
			return
		}

		// 先拿到第一条请求，再用一个很短的时间窗继续聚合。
		batch := []*writeReq{first}
		timer := time.NewTimer(writeBatchMaxDelay)
		closed := false

	collect:
		for len(batch) < writeBatchMaxCount {
			select {
			case req, ok := <-d.writeCh:
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

		d.applyWriteBatch(batch)
		if closed {
			return
		}
	}
}

// applyWriteBatch 在 WAL 和 MemTable 上原子地应用一批写请求。
func (d *DB) applyWriteBatch(batch []*writeReq) {
	d.mu.Lock()

	err := d.checkBGErrLocked()
	if err == nil {
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
				} else {
					rec.Op = 0
				}
				records = append(records, rec)
			}
		}

		// 先批量写 WAL，再按策略 fsync，最后按原顺序应用到 MemTable。
		if err = d.wal.AppendBatch(records); err == nil {
			d.writeSeq++

			if d.opt.walSync == WALSyncEveryBatch {
				err = d.syncWAL()
				if err == nil {
					d.syncedSeq = d.writeSeq
				}
			}
		}

		if err == nil {
			for _, req := range batch {
				for _, op := range req.ops {
					if op.op == writeOpDelete {
						d.mem.Delete(op.key)
					} else {
						d.mem.Put(op.key, op.value)
					}
				}
			}

			if d.shouldAutoFlushLocked() {
				d.requestAutoFlush()
			}
		} else {
			d.bgErr = err
		}
	}

	d.mu.Unlock()

	for _, req := range batch {
		req.done <- err
	}
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
		case <-d.syncStop:
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
	if d.writeSeq == d.syncedSeq {
		d.mu.Unlock()
		return nil
	}

	targetSeq := d.writeSeq
	d.mu.Unlock()

	if err := d.syncWAL(); err != nil {
		d.mu.Lock()
		if d.bgErr == nil {
			d.bgErr = err
		}
		d.mu.Unlock()
		return err
	}

	d.mu.Lock()
	if d.syncedSeq < targetSeq {
		d.syncedSeq = targetSeq
	}
	d.mu.Unlock()
	return nil
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
