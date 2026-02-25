package db

import (
	"fmt"
	"os"
	"path/filepath"

	"monolithdb/internal/memtable"
	"monolithdb/internal/sstable"
	"monolithdb/internal/wal"
)

const (
	numLevels = 3

	autoCompactThreshold = 4
	l0PickN              = 2
	l1PickN              = 1
	l1RunMaxEntries      = 256
	l1MaxRuns            = 8
)

type levelFile struct {
	id     uint64
	path   string
	minKey string
	maxKey string
}

type Level struct {
	id  int
	dir string

	// L0 用：newest-first，允许 overlap
	l0Paths []string

	// L>=1 用：按 minKey 升序，且不 overlap（由 compaction 保证）
	runs []levelFile

	// 配置
	maxFiles      int
	runMaxEntries int
}

type DB struct {
	mem *memtable.MemTable
	wal *wal.WAL

	dir     string
	walPath string
	sstDir  string

	levels []Level

	nextID uint64
}

func Open(dir string) (*DB, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	sstDir := filepath.Join(dir, "sst")
	if err := os.MkdirAll(sstDir, 0o755); err != nil {
		return nil, err
	}

	walPath := filepath.Join(dir, "forge.wal")

	w, err := wal.Open(walPath)
	if err != nil {
		return nil, err
	}

	m := memtable.NewMemTable()

	// 回放 WAL：把操作重新应用到 MemTable
	records, err := wal.Replay(walPath)
	if err != nil {
		_ = w.Close()
		return nil, err
	}
	for _, r := range records {
		switch r.Op {
		case 0:
			m.Put(r.Key, r.Value)
		case 1:
			m.Delete(r.Key)
		default:
			_ = w.Close()
			return nil, wal.ErrCorruptWAL
		}
	}

	levels := make([]Level, 0, numLevels)
	var maxID uint64 = 0

	for i := 0; i < numLevels; i++ {
		ldir := filepath.Join(sstDir, fmt.Sprintf("l%d", i))
		if err := os.MkdirAll(ldir, 0o755); err != nil {
			_ = w.Close()
			return nil, err
		}

		lv := Level{id: i, dir: ldir}

		if i == 0 {
			paths, mid, err := scanLevel0(ldir)
			if err != nil {
				_ = w.Close()
				return nil, err
			}
			lv.l0Paths = paths
			lv.maxFiles = autoCompactThreshold
			if mid > maxID {
				maxID = mid
			}
		} else {
			runs, mid, err := scanLevelN(ldir)
			if err != nil {
				_ = w.Close()
				return nil, err
			}
			lv.runs = runs
			lv.runMaxEntries = l1RunMaxEntries // 先统一用同一份配置
			if mid > maxID {
				maxID = mid
			}
		}

		levels = append(levels, lv)
	}

	nextID := maxID + 1

	return &DB{
		mem:     m,
		wal:     w,
		dir:     dir,
		walPath: walPath,
		sstDir:  sstDir,
		levels:  levels,
		nextID:  nextID,
	}, nil
}

func (d *DB) Close() error {
	if d.wal != nil {
		return d.wal.Close()
	}
	return nil
}

func (d *DB) Put(key string, value []byte) error {
	// 先写 WAL（Write-Ahead）
	if err := d.wal.AppendPut(key, value); err != nil {
		return err
	}
	// 再写 MemTable
	d.mem.Put(key, value)
	return nil
}

func (d *DB) Get(key string) ([]byte, bool, error) {
	// 1) MemTable
	if e, ok := d.mem.GetAll(key); ok {
		if e.Tombstone {
			return nil, false, nil
		}
		return e.Value, true, nil
	}

	// 2) Levels
	for li := 0; li < len(d.levels); li++ {
		lv := &d.levels[li]

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
	// 先写 WAL
	if err := d.wal.AppendDelete(key); err != nil {
		return err
	}
	// 再写 MemTable（tombstone）
	d.mem.Delete(key)
	return nil
}

func (d *DB) Flush() error {
	entries := d.mem.RangeAll("", "")
	if len(entries) == 0 {
		return nil
	}

	// 生成新 SSTable 文件名
	name := fmt.Sprintf("%06d.sst", d.nextID)
	path := filepath.Join(d.levels[0].dir, name)

	// 先写到临时文件，再 rename，避免写一半崩溃留下半成品
	tmp := path + ".tmp"
	if err := sstable.WriteTable(tmp, entries); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}

	// 把新表放到列表最前面
	d.levels[0].l0Paths = append([]string{path}, d.levels[0].l0Paths...)
	d.nextID++

	// 清空 MemTable
	d.mem = memtable.NewMemTable()

	// 截断 WAL：否则重启 Replay 会重复应用旧操作
	if err := d.wal.Close(); err != nil {
		return err
	}
	// 直接把 wal 文件清空
	if err := os.WriteFile(d.walPath, nil, 0o644); err != nil {
		return err
	}
	w, err := wal.Open(d.walPath)
	if err != nil {
		return err
	}
	d.wal = w

	// L0 -> L1
	if len(d.levels[0].l0Paths) > d.levels[0].maxFiles {
		if err := d.Compact(0); err != nil {
			return err
		}
	}

	// L1 -> L2
	if len(d.levels) > 2 && len(d.levels[1].runs) > l1MaxRuns {
		if err := d.Compact(1); err != nil {
			return err
		}
	}

	return nil
}
