package db

import (
	"errors"
	"time"

	"monolithdb/internal/wal"
)

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

var errDBClosed = errors.New("db: closed")

func (d *DB) submitWrite(op writeOp, key string, value []byte) error {
	return d.submitWriteBatch([]batchOp{{
		op:    op,
		key:   key,
		value: cloneWriteValue(value),
	}})
}

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
		// 保持 WAL-before-mem 语义。
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

func cloneWriteValue(value []byte) []byte {
	if value == nil {
		return nil
	}
	cp := make([]byte, len(value))
	copy(cp, value)
	return cp
}
