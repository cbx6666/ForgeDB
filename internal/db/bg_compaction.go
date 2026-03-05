package db

func (d *DB) compactionLoop() {
	defer d.wg.Done()

	for {
		// ===== 1) wait for request / closing (locked) =====
		d.mu.Lock()
		for !d.closing && !d.compactionReq {
			d.cond.Wait()
		}
		if d.closing {
			d.mu.Unlock()
			return
		}

		d.compactionReq = false
		d.compacting = true
		d.mu.Unlock()

		// ===== 2) drain compactions until no level is over limit =====
		for {
			// --- pick job (locked, short) ---
			d.mu.Lock()

			// 允许 Close 抢占退出
			if d.closing {
				d.compacting = false
				d.mu.Unlock()
				return
			}

			// 如果后台已经出错，停止继续 compact
			if d.bgErr != nil {
				d.compacting = false
				d.mu.Unlock()
				break
			}

			level, ok := d.pickCompactionLevelLocked()
			if !ok {
				d.compacting = false
				d.mu.Unlock()
				break
			}
			job, err := d.pickCompactionJobLocked(level)
			d.mu.Unlock()

			if err != nil {
				d.mu.Lock()
				d.bgErr = err
				d.compacting = false
				d.mu.Unlock()
				break
			}
			if job == nil {
				d.mu.Lock()
				d.compacting = false
				d.mu.Unlock()
				break
			}

			// --- do (unlocked, slow) ---
			res, err := d.doCompaction(job)
			if err != nil {
				d.mu.Lock()
				d.bgErr = err
				d.compacting = false
				d.mu.Unlock()
				break
			}

			// --- install (locked, short) ---
			d.mu.Lock()
			if err := d.installCompactionLocked(res); err != nil {
				d.bgErr = err
				d.compacting = false
				d.mu.Unlock()
				break
			}

			d.mu.Unlock()
		}
	}
}

func (d *DB) requestCompactionLocked() {
	d.compactionReq = true
	d.cond.Signal()
}

func (d *DB) checkBGErrLocked() error {
	if d.bgErr != nil {
		return d.bgErr
	}
	return nil
}
