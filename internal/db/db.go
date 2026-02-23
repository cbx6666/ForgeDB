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
	autoCompactThreshold = 4
	l0PickN              = 2
	l1RunMaxEntries      = 256
)

type levelFile struct {
	path   string
	minKey string
	maxKey string
}

type DB struct {
	mem *memtable.MemTable
	wal *wal.WAL

	dir     string
	walPath string
	sstDir  string
	l0Dir   string
	l1Dir   string

	l0 []string
	l1 []levelFile

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
	l0Dir := filepath.Join(sstDir, "l0")
	l1Dir := filepath.Join(sstDir, "l1")
	if err := os.MkdirAll(l0Dir, 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(l1Dir, 0o755); err != nil {
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

	l0, max0, err := scanSSTables(l0Dir)
	if err != nil {
		_ = w.Close()
		return nil, err
	}
	l1, max1, err := scanL1(l1Dir)
	if err != nil {
		_ = w.Close()
		return nil, err
	}
	nextID := max0
	if max1 > nextID {
		nextID = max1
	}
	nextID++

	return &DB{
		mem:     m,
		wal:     w,
		dir:     dir,
		walPath: walPath,
		sstDir:  sstDir,
		l0Dir:   l0Dir,
		l1Dir:   l1Dir,
		l0:      l0,
		l1:      l1,
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

	// 2) L0 (newest -> oldest)
	for _, p := range d.l0 {
		v, res, err := sstable.Get(p, key)
		if err != nil {
			return nil, false, err
		}
		switch res {
		case sstable.Found:
			return v, true, nil
		case sstable.Deleted:
			return nil, false, nil // 关键：删除短路，阻止旧值“复活”
		case sstable.NotFound:
			continue
		}
	}

	// 3) L1 (minKey 升序)
	if p, ok := d.findL1File(key); ok {
		v, res, err := sstable.Get(p, key)
		if err != nil {
			return nil, false, err
		}
		switch res {
		case sstable.Found:
			return v, true, nil
		case sstable.Deleted:
			return nil, false, nil
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
	path := filepath.Join(d.l0Dir, name)

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
	d.l0 = append([]string{path}, d.l0...)
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

	// 自动 compaction
	if len(d.l0) > autoCompactThreshold {
		if err := d.CompactL0ToL1(); err != nil {
			return err
		}
	}

	return nil
}
