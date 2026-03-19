package db

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"monolithdb/internal/manifest"
	"monolithdb/internal/memtable"
	"monolithdb/internal/sstable"
	"monolithdb/internal/wal"
)

type levelFile struct {
	id     uint64
	path   string
	minKey string
	maxKey string
}

type level struct {
	id  int
	dir string

	// L0 用：newest-first，允许 overlap
	l0Paths []string
	// L>=1 用：按 minKey 升序，且不 overlap（由 compaction 保证）
	runs []levelFile
}

type DB struct {
	mem *memtable.MemTable
	imm *memtable.MemTable
	wal *wal.WAL

	man            *manifest.Manifest
	manifestWrites int

	dir        string
	walPath    string
	immWalPath string // 冻结 memtable 对应的 WAL 文件路径
	sstDir     string

	current *Version
	opt     Options
	nextID  uint64

	// 写入批量合并：前台请求先进队列，由后台写协程做 group commit。
	writeMu     sync.RWMutex
	writeCh     chan *writeReq
	writeClosed bool
	syncWAL     func() error

	// bg compaction
	mu   sync.Mutex
	cond *sync.Cond
	wg   sync.WaitGroup

	closing       bool
	compactionReq bool
	compacting    bool
	bgErr         error
	pending       map[string]struct{} // path -> pending
}

type readView struct {
	version *Version
}

func Open(dir string) (*DB, error) {
	return OpenWithOptions(dir, defaultOptions())
}

func OpenWithOptions(dir string, opt Options) (*DB, error) {
	if err := validateOptions(opt); err != nil {
		return nil, err
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	sstDir := filepath.Join(dir, "sst")
	if err := os.MkdirAll(sstDir, 0o755); err != nil {
		return nil, err
	}

	manPath := filepath.Join(dir, "MANIFEST")
	man, err := manifest.Open(manPath)
	if err != nil {
		return nil, err
	}

	walPath := filepath.Join(dir, "forge.wal")
	immWalPath := filepath.Join(dir, "forge.flush.wal")

	w, err := wal.Open(walPath)
	if err != nil {
		_ = man.Close()
		return nil, err
	}

	m := memtable.NewMemTable()

	// 回放 WAL：把操作重新应用到 MemTable
	replayInto := func(path string, m *memtable.MemTable) error {
		records, err := wal.Replay(path)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		for _, r := range records {
			switch r.Op {
			case 0:
				m.Put(r.Key, r.Value)
			case 1:
				m.Delete(r.Key)
			default:
				return wal.ErrCorruptWAL
			}
		}
		return nil
	}

	if err := replayInto(immWalPath, m); err != nil {
		_ = w.Close()
		_ = man.Close()
		return nil, err
	}
	if err := replayInto(walPath, m); err != nil {
		_ = w.Close()
		_ = man.Close()
		return nil, err
	}

	var levels []level
	var nextID uint64

	levels, nextID, err = recoverFromManifest(manPath, sstDir, opt)
	if err != nil {
		// 只有两类错误允许回退：没有 snapshot / manifest 文件不存在
		if errors.Is(err, manifest.ErrNoSnapshot) || os.IsNotExist(err) {
			levels, nextID, err = scanAllLevels(sstDir, opt)
			if err != nil {
				_ = w.Close()
				_ = man.Close()
				return nil, err
			}

			// 第一次启动：用 scan 恢复后，立刻把结构写入 MANIFEST
			if err := man.WriteSnapshot(nextID, levelsToSnapshot(levels)); err != nil {
				_ = w.Close()
				_ = man.Close()
				return nil, err
			}
		} else {
			_ = w.Close()
			_ = man.Close()
			return nil, err
		}
	}

	orphan, err := sanityCheckSSTDir(sstDir, levels)
	if err != nil {
		_ = w.Close()
		_ = man.Close()
		return nil, err
	}
	if len(orphan) > 0 {
		show := orphan
		if len(show) > 10 {
			show = show[:10]
		}
		fmt.Printf("WARN: db: found orphan sst files (not referenced by manifest): %v (total=%d)\n", show, len(orphan))
	}

	db := &DB{
		mem:        m,
		wal:        w,
		man:        man,
		dir:        dir,
		walPath:    walPath,
		immWalPath: immWalPath,
		sstDir:     sstDir,
		current:    newVersionFromLevels(levels),
		opt:        opt,
		nextID:     nextID,
		pending:    make(map[string]struct{}),
		writeCh:    make(chan *writeReq, 256),
		syncWAL:    w.Sync,
	}
	db.cond = sync.NewCond(&db.mu)

	// 分别覆盖 compaction / write loop 的整个生命周期
	db.wg.Add(1)
	go db.compactionLoop()
	db.wg.Add(1)
	go db.writeLoop()

	return db, nil
}

func (d *DB) Close() error {
	d.mu.Lock()
	d.closing = true
	if d.cond != nil {
		// 唤醒所有等待的协程并结束生命周期
		d.cond.Broadcast()
	}
	d.mu.Unlock()

	d.writeMu.Lock()
	if !d.writeClosed {
		d.writeClosed = true
		close(d.writeCh)
	}
	d.writeMu.Unlock()

	// 保证在执行接下来的“关闭文件”动作前，没有任何后台协程还在尝试读写这些文件
	d.wg.Wait()

	var err1, err2 error
	if d.man != nil {
		err1 = d.man.Close()
	}
	if d.wal != nil {
		err2 = d.wal.Close()
	}
	if err1 != nil {
		return err1
	}
	return err2
}

func (d *DB) Put(key string, value []byte) error {
	return d.submitWrite(writeOpPut, key, value)
}

func (d *DB) Get(key string) ([]byte, bool, error) {
	d.mu.Lock()
	if err := d.checkBGErrLocked(); err != nil {
		d.mu.Unlock()
		return nil, false, err
	}

	// 1) MemTable
	if e, ok := d.mem.GetAll(key); ok {
		d.mu.Unlock()
		if e.Tombstone {
			return nil, false, nil
		}
		return e.Value, true, nil
	}

	// 2) immutable MemTable (flush in progress)
	if d.imm != nil {
		if e, ok := d.imm.GetAll(key); ok {
			d.mu.Unlock()
			if e.Tombstone {
				return nil, false, nil
			}
			return e.Value, true, nil
		}
	}

	// 3) 抓取当前 Version，后续锁外查 SST
	view := readView{
		version: d.currentVersion(),
	}
	d.mu.Unlock()

	levels := view.version.levels

	// 4) Levels
	for li := 0; li < len(levels); li++ {
		lv := &levels[li]

		if li == 0 {
			// L0 newest-first
			for _, p := range lv.l0Paths {
				v, res, err := sstable.Get(p, key)
				if err != nil {
					return nil, false, err
				}
				switch res {
				case sstable.Found:
					return v, true, nil
				case sstable.Deleted:
					return nil, false, nil
				case sstable.NotFound:
					continue
				}
			}
		} else {
			// L>=1: run locate
			if p, ok := findRun(lv.runs, key); ok {
				v, res, err := sstable.Get(p, key)
				if err != nil {
					return nil, false, err
				}
				switch res {
				case sstable.Found:
					return v, true, nil
				case sstable.Deleted:
					return nil, false, nil
				case sstable.NotFound:
					continue // 继续查更旧层
				}
			}
		}
	}

	return nil, false, nil
}

func (d *DB) Delete(key string) error {
	return d.submitWrite(writeOpDelete, key, nil)
}

// Flush flushes current MemTable to a new L0 SST.
// It rotates active memtable/WAL first so foreground writes can continue.
// It does NOT guarantee compaction completion; compaction runs asynchronously.
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

	// slow IO out of lock
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

func (d *DB) persistManifestLevels(levels []level) error {
	if d.man == nil {
		return nil
	}

	// 1) 追加写入 snapshot
	if err := d.man.WriteSnapshot(d.nextID, levelsToSnapshot(levels)); err != nil {
		return err
	}

	// 2) 达到阈值就做一次 checkpoint（压缩重写）
	d.manifestWrites++

	const checkpointEvery = 50
	if d.manifestWrites >= checkpointEvery {
		if err := d.man.CheckpointLatest(); err != nil {
			return err
		}
		d.manifestWrites = 0
	}

	return nil
}

func (d *DB) currentVersion() *Version {
	if d.current == nil {
		return &Version{}
	}
	return d.current
}
