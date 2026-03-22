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
	// entries 是实际要落盘的有序记录；由 doFlush 在开始写 SST 前填充。
	entries []types.Entry
	// gen 是这轮 flush 的代际号，用来让等待方准确识别“自己等的那一轮”。
	gen uint64
	// id 是将要分配给目标 SST 的文件号。
	id uint64
	// path 是最终 SST 路径，tmpPath 是写盘阶段使用的临时文件路径。
	path    string
	tmpPath string
}

// db_flush.go 收拢 flush 相关的前台入口、后台循环和状态转换。

// Flush 将当前 memtable 刷成新的 L0 SST。
func (d *DB) Flush() error {
	for {
		d.mu.Lock()
		if err := d.checkBGErrLocked(); err != nil {
			d.mu.Unlock()
			return err
		}
		// 一旦进入关闭流程，就不再允许前台发起新的显式 Flush。
		if d.closing {
			d.mu.Unlock()
			return errDBClosed
		}

		// 已经没有待刷数据时直接返回。
		if d.imm == nil && !d.flush.running && (d.mem == nil || d.mem.ApproxSize() == 0) {
			d.mu.Unlock()
			return nil
		}

		// 如果已经有一轮 flush 在进行，先等它结束，再决定是否还要继续刷当前 mem。
		if d.flush.running || d.flush.job != nil {
			if d.flush.job != nil && !d.flush.running {
				// 可能已经 prepare 出了 job，但后台 worker 还没开始执行，
				// 这里补一次唤醒，确保 flushLoop 会去接这份任务。
				d.requestFlushLocked()
			}

			// 记住当前这轮 flush 的代际号，只等待这一轮完成。
			// doneGen 只有在 install 成功后才会推进，因此它表示
			// “我正在等的这轮 flush 是否真的已经结束”。
			target := d.flush.gen
			for d.bgErr == nil && d.flush.doneGen < target {
				d.flush.cond.Wait()
			}
			err := d.checkBGErrLocked()
			d.mu.Unlock()
			if err != nil {
				return err
			}
			// 当前在途的那轮 flush 已经结束，但等待期间可能又积累了新的 mem 数据，
			// 因此回到循环顶部重新判断，而不是在这里直接返回。
			continue
		}

		before := d.flush.doneGen
		// 当前还没有正在执行的 flush，则发起一次请求并等待至少一轮完成。
		d.requestFlushLocked()
		for d.bgErr == nil && d.flush.doneGen <= before {
			// 如果等待期间发现已经没有待刷数据了，就不用继续阻塞。
			if d.imm == nil && !d.flush.running && d.flush.job == nil && (d.mem == nil || d.mem.ApproxSize() == 0) {
				break
			}
			d.flush.cond.Wait()
		}
		err := d.checkBGErrLocked()
		d.mu.Unlock()
		if err != nil {
			return err
		}
		// 当前至少完成了一轮 flush，且没有新的待刷数据需要继续处理。
		return nil
	}
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

// requestFlushLocked 在锁内标记并唤醒后台 flush。
func (d *DB) requestFlushLocked() {
	d.flush.req = true
	if d.flush.cond != nil {
		// flushLoop 和显式 Flush 都可能在这上面等待，因此这里只做一次 signal。
		d.flush.cond.Signal()
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
		// 这里只做阈值判断和请求投递，真正的 flush 仍统一由 flushLoop 执行。
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
		// 满足阈值后只负责发起 flush 请求，不在这个 goroutine 里直接做磁盘 I/O。
		d.requestFlushLocked()
		d.mu.Unlock()
	}
}

// flushLoop 在后台串行执行单 imm flush。
func (d *DB) flushLoop() {
	defer d.wg.Done()

	for {
		d.mu.Lock()
		// flushLoop 的等待谓词很简单：DB 关闭，或者存在待处理的 flush 请求。
		for !d.closing && !d.flush.req {
			d.flush.cond.Wait()
		}

		if d.bgErr != nil {
			// 后台错误一旦出现，后续 flush 不再继续推进；先广播唤醒等待方再退出。
			d.flush.cond.Broadcast()
			d.mu.Unlock()
			return
		}
		if d.closing && !d.flush.req {
			// 关闭流程里如果没有待处理请求，就可以安全退出 flushLoop。
			d.flush.cond.Broadcast()
			d.mu.Unlock()
			return
		}

		// 先看是否已有 prepare 好的 job；如果没有，就由 flushLoop 自己补做 prepare。
		job := d.flush.job
		if job == nil {
			var err error
			// 由后台统一完成 freeze 和构建任务，前台路径只负责发请求。
			job, err = d.prepareFlushLocked()
			d.flush.req = false
			if err != nil {
				d.bgErr = err
				// prepare 失败同样视为后台错误，后续操作统一通过 bgErr 暴露。
				d.flush.cond.Broadcast()
				d.mu.Unlock()
				return
			}
			if job == nil {
				d.flush.cond.Broadcast()
				d.mu.Unlock()
				continue
			}
		} else {
			// 已经有可执行任务时，消费掉当前这次“待处理请求”标记。
			d.flush.req = false
		}

		// 进入真正的 I/O 阶段前，把运行态打开并尽快释放锁。
		d.flush.running = true
		d.mu.Unlock()

		err := d.doFlush(job)

		d.mu.Lock()
		if err == nil {
			// 只有当 SST 成功写出后，才进入锁内安装新版本并清理中间态。
			err = d.installFlushLocked()
		}
		if err != nil && d.bgErr == nil {
			d.bgErr = err
		}
		d.flush.running = false
		if err == nil && d.shouldAutoFlushLocked() {
			// 如果这轮 flush 完成后，新 active mem 又已经超阈值，就继续排下一轮。
			d.flush.req = true
		}
		// 无论成功、失败还是退出，都要广播唤醒显式 Flush 和其他等待者重新判断状态。
		d.flush.cond.Broadcast()
		shouldExit := d.bgErr != nil || (d.closing && !d.flush.req)
		d.mu.Unlock()

		if shouldExit {
			return
		}
	}
}

// prepareFlushLocked 冻结当前 memtable，切 WAL，并返回 flush 任务。
func (d *DB) prepareFlushLocked() (*flushJob, error) {
	if d.imm != nil || d.flush.job != nil {
		// 当前实现仍然是单 imm 模型，因此一旦已有冻结表或待安装 job，就不能再 prepare。
		return nil, fmt.Errorf("db: flush already in progress")
	}

	if d.mem == nil || d.mem.ApproxSize() == 0 {
		// 没有有效数据时不生成空的 flush 任务。
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
		// 轮转失败时，把 mem/imm 状态完整回滚到 prepare 前。
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
		// rename 失败同样要回滚前面切出去的 active mem。
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
		// 新 WAL 打不开时，也必须恢复成 prepare 前的写入布局。
		d.mem = oldMem
		d.imm = nil
		return nil, err
	}
	d.wal = newWal

	if err := syncDirFn(d.dir); err != nil {
		return nil, err
	}

	job := &flushJob{
		gen:     d.flush.gen + 1,
		id:      id,
		path:    path,
		tmpPath: tmpPath,
	}
	// gen 和 job 一起推进，表示“这一轮 flush 已经正式创建，可供后台执行”。
	d.flush.gen = job.gen
	d.flush.job = job
	return job, nil
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
		// 当前实现不会为纯空表生成 SST，避免制造无意义文件。
		return fmt.Errorf("db: flush job has no entries")
	}

	// 先写临时文件，再 rename 成最终 SST，避免中途失败留下半成品。
	if err := sstable.WriteTable(job.tmpPath, job.entries); err != nil {
		_ = os.Remove(job.tmpPath)
		return err
	}
	if err := os.Rename(job.tmpPath, job.path); err != nil {
		_ = os.Remove(job.tmpPath)
		return err
	}
	// rename 之后还要同步父目录，确保目录项更新在崩溃后可恢复。
	if err := syncPathDir(job.path); err != nil {
		return err
	}
	return nil
}

// installFlushLocked 把当前 flush job 对应的新 SST 安装到 L0，并清理 flush 中间态。
func (d *DB) installFlushLocked() error {
	job := d.flush.job
	if job == nil {
		return fmt.Errorf("db: no flush job to install")
	}

	oldv := d.currentVersion()
	newv := oldv.withLevels()

	// 挂到 L0 头部。
	newv.levels[0].l0Paths = append([]string{job.path}, newv.levels[0].l0Paths...)
	d.nextID++

	// manifest 先落盘，再切换 current，保持“先持久化元数据，再对外可见”的顺序。
	if err := d.persistManifestLevels(newv.levels); err != nil {
		return err
	}

	// manifest 持久化成功后，当前版本才真正切换为新视图。
	d.current = newv

	// flush 成功后，冻结 memtable 和 flush WAL 都可以清理掉。
	d.imm = nil
	d.flush.job = nil
	d.flush.doneGen = job.gen
	// flush WAL 只有在 install 成功后才能删除；否则重启还需要依赖它恢复。
	if err := removeFileAndSync(d.immWalPath); err != nil {
		return err
	}

	// 唤醒后台 compaction。
	d.requestCompactionLocked()
	return nil
}
