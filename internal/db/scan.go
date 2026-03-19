package db

import (
	"sort"

	"monolithdb/internal/sstable"
	"monolithdb/internal/types"
)

// Iterator 提供一个简单的只读范围迭代器。
// 当前实现会先把范围结果物化到内存中，再按序遍历。
type Iterator struct {
	entries []types.Entry
	idx     int
}

func (it *Iterator) Valid() bool {
	return it != nil && it.idx < len(it.entries)
}

func (it *Iterator) Entry() types.Entry {
	if !it.Valid() {
		return types.Entry{}
	}
	return it.entries[it.idx]
}

func (it *Iterator) Next() error {
	if it.Valid() {
		it.idx++
	}
	return nil
}

func (it *Iterator) Close() error { return nil }

// Scan 返回 [start, end) 范围内当前可见的有序记录。
// 返回结果会完成多层去重，并过滤 tombstone。
func (d *DB) Scan(start, end string) ([]types.Entry, error) {
	if end != "" && start != "" && start >= end {
		return nil, nil
	}

	// 先在锁下抓取一个范围快照：当前 mem、imm，以及当前 version。
	// 后续真正的 SST 扫描都放在锁外做，避免长时间阻塞前台路径。
	memEntries, immEntries, view, err := d.captureScanSnapshot(start, end)
	if err != nil {
		return nil, err
	}

	// sources 的顺序就是“可见性优先级”：
	// mem > imm > L0(newest-first) > L1+。
	// 后面 merge 时，先出现的来源会屏蔽后面的旧值。
	sources := make([][]types.Entry, 0, 2+len(view.version.levels))
	sources = append(sources, memEntries)
	if len(immEntries) > 0 {
		sources = append(sources, immEntries)
	}

	levels := view.version.levels
	for li := 0; li < len(levels); li++ {
		lv := &levels[li]

		if li == 0 {
			// L0 允许 overlap，而且内存里本来就是 newest-first。
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

		// L>=1 的 run 不 overlap，先按范围过滤一遍，避免无意义的整表扫描。
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

func (d *DB) captureScanSnapshot(start, end string) ([]types.Entry, []types.Entry, readView, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if err := d.checkBGErrLocked(); err != nil {
		return nil, nil, readView{}, err
	}

	memEntries := d.mem.RangeAll(start, end)
	var immEntries []types.Entry
	if d.imm != nil {
		immEntries = d.imm.RangeAll(start, end)
	}

	view := readView{
		version: d.currentVersion(),
	}
	return memEntries, immEntries, view, nil
}

func scanSSTRange(path string, start, end string) ([]types.Entry, error) {
	it, err := sstable.NewIter(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()

	var out []types.Entry
	for it.Valid() {
		ent := it.Entry()

		// 迭代器本身是全表顺序扫描，这里只保留范围内的部分。
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
			Value:     cloneScanBytes(ent.Value),
			Tombstone: ent.Tombstone,
		})
		if err := it.Next(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func runOverlapsRange(run levelFile, start, end string) bool {
	if start != "" && run.maxKey < start {
		return false
	}
	if end != "" && run.minKey >= end {
		return false
	}
	return true
}

func mergeScanSources(sources [][]types.Entry) []types.Entry {
	seen := make(map[string]types.Entry)
	for _, src := range sources {
		for _, ent := range src {
			// 第一次出现的 key 就是当前可见版本，后面的旧值直接忽略。
			if _, ok := seen[ent.Key]; ok {
				continue
			}
			seen[ent.Key] = ent
		}
	}

	keys := make([]string, 0, len(seen))
	for key, ent := range seen {
		// tombstone 只负责遮蔽旧值，不应出现在最终 Scan 结果里。
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
			Value:     cloneScanBytes(ent.Value),
			Tombstone: false,
		})
	}
	return out
}

func cloneScanBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	cp := make([]byte, len(b))
	copy(cp, b)
	return cp
}
