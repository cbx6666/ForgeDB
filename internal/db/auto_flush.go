package db

func (d *DB) requestAutoFlush() {
	if d.autoFlushCh == nil {
		return
	}
	select {
	case d.autoFlushCh <- struct{}{}:
	default:
	}
}

func (d *DB) shouldAutoFlushLocked() bool {
	return d.opt.autoFlushBytes > 0 &&
		d.imm == nil &&
		d.mem != nil &&
		d.mem.ApproxSize() >= d.opt.autoFlushBytes
}

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

		// 检查前台写入
		if d.shouldAutoFlushLocked() {
			d.requestAutoFlush()
		}
		d.mu.Unlock()
	}
}
