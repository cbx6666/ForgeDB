package db

// db_compaction.go 负责 compaction 的调度、执行和安装逻辑。

// RequestCompaction 主动请求一次后台 compaction。
func (d *DB) RequestCompaction() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.checkBGErrLocked(); err != nil {
		return err
	}
	d.requestCompactionLocked()
	return nil
}

// compactionLoop 在后台持续处理 compaction 请求。
func (d *DB) compactionLoop() {
	defer d.wg.Done()

	for {
		// 1) 等待请求或关闭信号。
		d.mu.Lock()
		for !d.closing && !d.compaction.compactionReq {
			d.compaction.cond.Wait()
		}
		if d.closing {
			d.mu.Unlock()
			return
		}

		d.compaction.compactionReq = false
		d.mu.Unlock()

		// 2) 持续压实，直到没有任何层超阈值。
		for d.runCompactionStep() {
		}
	}
}

// runCompactionStep 执行一轮单 worker compaction。
// 返回 true 表示本轮确实完成了一次 pick/do/install，可继续尝试下一轮。
// 返回 false 表示当前没有更多可做的工作，或者后台已经进入停止/错误状态。
func (d *DB) runCompactionStep() bool {
	d.mu.Lock()

	if d.closing || d.bgErr != nil {
		d.mu.Unlock()
		return false
	}

	level, ok := d.pickCompactionLevelLocked()
	if !ok {
		// 当前已经没有层超阈值，这一轮后台 compaction 可以先收住。
		d.mu.Unlock()
		return false
	}
	job, err := d.pickCompactionJobLocked(level)
	d.mu.Unlock()

	if err != nil {
		d.mu.Lock()
		// 一旦选任务出错，后续操作统一通过 bgErr 快速失败。
		d.failCompactionLocked(nil, err)
		d.mu.Unlock()
		return false
	}
	if job == nil {
		return false
	}

	newRuns, err := d.doCompaction(job)
	if err != nil {
		d.mu.Lock()
		// 执行阶段失败时必须清掉 pending，避免后续该范围永久不可选。
		d.failCompactionLocked(job, err)
		d.mu.Unlock()
		return false
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.installCompactionLocked(job, newRuns); err != nil {
		// 安装失败同样要先回收 pending，再把错误上升到全局后台错误。
		d.failCompactionLocked(job, err)
		return false
	}
	return true
}

// requestCompactionLocked 在锁内标记并唤醒后台 compaction。
func (d *DB) requestCompactionLocked() {
	d.compaction.compactionReq = true
	// compactionLoop 只有一个消费者，这里 signal 一个等待者即可。
	d.compaction.cond.Signal()
}

// checkBGErrLocked 返回后台协程记录的首个错误。
func (d *DB) checkBGErrLocked() error {
	if d.bgErr != nil {
		return d.bgErr
	}
	return nil
}
