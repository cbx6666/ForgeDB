package db

import (
	"os"
	"path/filepath"

	"monolithdb/internal/memtable"
	"monolithdb/internal/wal"
)

type DB struct {
	mem *memtable.MemTable
	wal *wal.WAL
	dir string
}

func Open(dir string) (*DB, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	walPath := filepath.Join(dir, "forge.wal")

	w, err := wal.Open(walPath)
	if err != nil {
		return nil, err
	}

	m := memtable.NewMemTable()

	// // 回放 WAL：把操作重新应用到 MemTable
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

	return &DB{
		mem: m,
		wal: w,
		dir: dir,
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

func (d *DB) Get(key string) ([]byte, bool) {
	return d.mem.Get(key)
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
