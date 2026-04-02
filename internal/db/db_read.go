package db

import (
	"sort"

	"monolithdb/internal/sstable"
	"monolithdb/internal/types"
)

// db_read.go 负责点查、范围读和迭代器逻辑。

// Get 按 active memtable、immutable memtable、SST 的顺序执行点查。
func (d *DB) Get(key string) ([]byte, bool, error) {
	view, err := d.capturePointReadView(key)
	if err != nil {
		return nil, false, err
	}

	// 1) 先查 active memtable 的快照结果。
	if view.memFound {
		if view.memEntry.Tombstone {
			return nil, false, nil
		}
		return cloneReadBytes(view.memEntry.Value), true, nil
	}

	// 2) 再按 newest-first 查 immutable memtable 队列。
	for i := len(view.imms) - 1; i >= 0; i-- {
		imm := view.imms[i]
		if imm == nil || imm.mem == nil {
			continue
		}
		if e, ok := imm.mem.GetAll(key); ok {
			if e.Tombstone {
				return nil, false, nil
			}
			return cloneReadBytes(e.Value), true, nil
		}
	}

	// 3) 最后在锁外查当前版本对应的 SST。
	levels := view.version.levels
	for li := 0; li < len(levels); li++ {
		lv := &levels[li]

		if li == 0 {
			// L0 允许重叠，按 newest-first 查找。
			for _, p := range lv.l0Paths {
				v, res, err := d.cache.Get(p, key)
				if err != nil {
					return nil, false, err
				}
				switch res {
				case sstable.Found:
					return cloneReadBytes(v), true, nil
				case sstable.Deleted:
					return nil, false, nil
				case sstable.NotFound:
					continue
				}
			}
			continue
		}

		// L1 及以上先定位 run，再做单表查询。
		if p, ok := findRun(lv.runs, key); ok {
			v, res, err := d.cache.Get(p, key)
			if err != nil {
				return nil, false, err
			}
			switch res {
			case sstable.Found:
				return cloneReadBytes(v), true, nil
			case sstable.Deleted:
				return nil, false, nil
			case sstable.NotFound:
				continue
			}
		}
	}

	return nil, false, nil
}

// Scan 返回 [start, end) 范围内当前可见的有序记录。
// 返回结果会完成多层去重，并过滤 tombstone。
func (d *DB) Scan(start, end string) ([]types.Entry, error) {
	if end != "" && start != "" && start >= end {
		return nil, nil
	}

	// 先在锁下抓取一次范围读快照：active mem 的物化结果、immutable queue 和当前 version。
	// 后续真正的 SST 扫描都放在锁外做，避免长时间阻塞前台路径。
	view, err := d.captureScanSnapshot(start, end)
	if err != nil {
		return nil, err
	}

	// sources 的顺序就是“可见性优先级”：
	// active mem > immutable queue(newest-first) > L0(newest-first) > L1+。
	sources := make([][]types.Entry, 0, 2+len(view.imms)+len(view.version.levels))
	sources = append(sources, view.memEntries)
	for i := len(view.imms) - 1; i >= 0; i-- {
		imm := view.imms[i]
		if imm == nil || imm.mem == nil {
			continue
		}
		immEntries := imm.mem.RangeAll(start, end)
		if len(immEntries) > 0 {
			sources = append(sources, immEntries)
		}
	}

	levels := view.version.levels
	for li := 0; li < len(levels); li++ {
		lv := &levels[li]

		if li == 0 {
			// L0 允许 overlap，而且内存中的顺序本来就是 newest-first。
			for _, p := range lv.l0Paths {
				entries, err := scanSSTRange(p, start, end)
				if err != nil {
					return nil, err
				}
				if len(entries) > 0 {
					sources = append(sources, entries)
				}
			}
			continue
		}

		// L1 及以上 run 不重叠，先按范围过滤一遍。
		for _, run := range lv.runs {
			if !runOverlapsRange(run, start, end) {
				continue
			}
			entries, err := scanSSTRange(run.path, start, end)
			if err != nil {
				return nil, err
			}
			if len(entries) > 0 {
				sources = append(sources, entries)
			}
		}
	}

	return mergeScanSources(sources), nil
}

// NewIterator 返回 [start, end) 范围的迭代器。
func (d *DB) NewIterator(start, end string) (*Iterator, error) {
	entries, err := d.Scan(start, end)
	if err != nil {
		return nil, err
	}
	return &Iterator{entries: entries}, nil
}

// Iterator 提供一个简单的只读范围迭代器。
// 当前实现会先把范围结果物化到内存中，再按顺序遍历。
type Iterator struct {
	entries []types.Entry
	idx     int
}

// Valid 返回当前迭代器位置是否有效。
func (it *Iterator) Valid() bool {
	return it != nil && it.idx < len(it.entries)
}

// Entry 返回当前迭代器位置上的记录。
func (it *Iterator) Entry() types.Entry {
	if !it.Valid() {
		return types.Entry{}
	}
	return it.entries[it.idx]
}

// Next 将迭代器推进到下一条记录。
func (it *Iterator) Next() error {
	if it.Valid() {
		it.idx++
	}
	return nil
}

// Close 释放迭代器资源。
func (it *Iterator) Close() error { return nil }

// capturePointReadView 在锁下抓取一次点查所需的快照。
func (d *DB) capturePointReadView(key string) (pointReadView, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if err := d.checkBGErrLocked(); err != nil {
		return pointReadView{}, err
	}

	view := pointReadView{
		version: d.currentVersion(),
	}
	if len(d.imms) > 0 {
		view.imms = append([]*immutableMem(nil), d.imms...)
	}
	if d.mem != nil {
		view.memEntry, view.memFound = d.mem.GetAll(key)
	}
	return view, nil
}

// captureScanSnapshot 在锁下抓取一次范围读所需的快照。
func (d *DB) captureScanSnapshot(start, end string) (scanReadView, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if err := d.checkBGErrLocked(); err != nil {
		return scanReadView{}, err
	}

	view := scanReadView{
		version: d.currentVersion(),
	}
	if len(d.imms) > 0 {
		view.imms = append([]*immutableMem(nil), d.imms...)
	}
	if d.mem != nil {
		// active memtable 仍可能继续写入，因此这里先在锁下把范围结果物化出来。
		view.memEntries = d.mem.RangeAll(start, end)
	}
	return view, nil
}

// scanSSTRange 扫描单张 SST 的指定范围。
func scanSSTRange(path string, start, end string) ([]types.Entry, error) {
	it, err := sstable.NewIter(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()

	var out []types.Entry
	for it.Valid() {
		ent := it.Entry()

		if start != "" && ent.Key < start {
			if err := it.Next(); err != nil {
				return nil, err
			}
			continue
		}
		if end != "" && ent.Key >= end {
			break
		}

		out = append(out, types.Entry{
			Key:       ent.Key,
			Value:     cloneReadBytes(ent.Value),
			Tombstone: ent.Tombstone,
		})
		if err := it.Next(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// runOverlapsRange 判断某个 run 是否和给定范围有交集。
func runOverlapsRange(run levelFile, start, end string) bool {
	if start != "" && run.maxKey < start {
		return false
	}
	if end != "" && run.minKey >= end {
		return false
	}
	return true
}

// mergeScanSources 按可见性优先级合并多个来源的范围扫描结果。
func mergeScanSources(sources [][]types.Entry) []types.Entry {
	seen := make(map[string]types.Entry)
	for _, src := range sources {
		for _, ent := range src {
			if _, ok := seen[ent.Key]; ok {
				continue
			}
			seen[ent.Key] = ent
		}
	}

	keys := make([]string, 0, len(seen))
	for key, ent := range seen {
		if ent.Tombstone {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)

	out := make([]types.Entry, 0, len(keys))
	for _, key := range keys {
		ent := seen[key]
		out = append(out, types.Entry{
			Key:       ent.Key,
			Value:     cloneReadBytes(ent.Value),
			Tombstone: false,
		})
	}
	return out
}

// cloneReadBytes 复制读路径返回的值，避免把内部底层切片暴露给调用方。
func cloneReadBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	cp := make([]byte, len(b))
	copy(cp, b)
	return cp
}
