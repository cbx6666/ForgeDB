package db

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"monolithdb/internal/memtable"
	"monolithdb/internal/sstable"
	"monolithdb/internal/wal"
)

type DB struct {
	mem *memtable.MemTable
	wal *wal.WAL

	dir     string
	walPath string
	sstDir  string

	sstables []string
	nextID   uint64
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

	sstables, nextID, err := scanSSTables(sstDir)
	if err != nil {
		_ = w.Close()
		return nil, err
	}

	return &DB{
		mem:      m,
		wal:      w,
		dir:      dir,
		walPath:  walPath,
		sstDir:   sstDir,
		sstables: sstables,
		nextID:   nextID,
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
	// 1) MemTable
	if e, ok := d.mem.GetAll(key); ok {
		if e.Tombstone {
			return nil, false
		}
		return e.Value, true
	}

	// 2) SSTables (newest -> oldest)
	for _, p := range d.sstables {
		v, res, err := sstable.Get(p, key)
		if err != nil {
			return nil, false
		}
		switch res {
		case sstable.Found:
			return v, true
		case sstable.Deleted:
			return nil, false // 关键：删除短路，阻止旧值“复活”
		case sstable.NotFound:
			continue
		}
	}

	return nil, false
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
	path := filepath.Join(d.sstDir, name)

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
	d.sstables = append([]string{path}, d.sstables...)
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

	return nil
}

func scanSSTables(sstDir string) (paths []string, nextID uint64, err error) {
	// 匹配这个目录下所有以 .sst 结尾的文件名
	glob := filepath.Join(sstDir, "*.sst")
	list, err := filepath.Glob(glob)
	if err != nil {
		return nil, 1, err
	}

	sort.Strings(list)

	var maxID uint64 = 0
	for _, p := range list {
		id, ok := parseSSTID(p)
		if ok && id > maxID {
			maxID = id
		}
	}

	// 内存里用 newest-first，所以反转
	for i, j := 0, len(list)-1; i < j; i, j = i+1, j-1 {
		list[i], list[j] = list[j], list[i]
	}

	return list, maxID + 1, nil
}

func parseSSTID(path string) (uint64, bool) {
	base := filepath.Base(path)                // 000001.sst
	name := strings.TrimSuffix(base, ".sst")   // 000001
	id, err := strconv.ParseUint(name, 10, 64) // 把字符串解析成无符号整数，base：进制，bitSize：目标位宽
	if err != nil {
		return 0, false
	}
	return id, true
}
