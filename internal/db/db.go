package db

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

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
	wal *wal.WAL
	man *manifest.Manifest

	dir     string
	walPath string
	sstDir  string

	levels []level
	opt    Options
	nextID uint64
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

	w, err := wal.Open(walPath)
	if err != nil {
		_ = man.Close()
		return nil, err
	}

	m := memtable.NewMemTable()

	// 回放 WAL：把操作重新应用到 MemTable
	records, err := wal.Replay(walPath)
	if err != nil {
		_ = w.Close()
		_ = man.Close()
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
			_ = man.Close()
			return nil, wal.ErrCorruptWAL
		}
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

	return &DB{
		mem:     m,
		wal:     w,
		man:     man,
		dir:     dir,
		walPath: walPath,
		sstDir:  sstDir,
		levels:  levels,
		opt:     opt,
		nextID:  nextID,
	}, nil
}

func (d *DB) Close() error {
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

	if err := d.persistManifest(); err != nil {
		return err
	}

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

	// compaction
	for level := 0; level < len(d.levels)-1; level++ {
		maxFiles := d.opt.levels[level].maxFiles
		if maxFiles <= 0 {
			continue
		}

		over := false
		if level == 0 {
			over = len(d.levels[level].l0Paths) > maxFiles
		} else {
			over = len(d.levels[level].runs) > maxFiles
		}

		if over {
			if err := d.Compact(level); err != nil {
				return err
			}
		}
	}

	return nil
}

func (d *DB) persistManifest() error {
	if d.man == nil {
		return nil
	}
	return d.man.WriteSnapshot(d.nextID, levelsToSnapshot(d.levels))
}
