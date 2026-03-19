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

type writeReq struct {
	op    writeOp
	key   string
	value []byte
	done  chan error
}

var errDBClosed = errors.New("db: closed")

func (d *DB) submitWrite(op writeOp, key string, value []byte) error {
	req := &writeReq{
		op:    op,
		key:   key,
		value: cloneWriteValue(value),
		done:  make(chan error, 1),
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

	// 标签控制流
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

		if !timer.Stop() { // 时钟信号已经发送
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
		records := make([]wal.Record, len(batch))
		for i, req := range batch {
			records[i] = wal.Record{
				Key:   req.key,
				Value: req.value,
			}
			if req.op == writeOpDelete {
				records[i].Op = 1
			} else {
				records[i].Op = 0
			}
		}

		// 先批量写 WAL，再按策略 fsync，最后按原顺序应用到 MemTable，
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
				if req.op == writeOpDelete {
					d.mem.Delete(req.key)
				} else {
					d.mem.Put(req.key, req.value)
				}
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
