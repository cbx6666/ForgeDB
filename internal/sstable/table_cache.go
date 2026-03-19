package sstable

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
)

// TableMeta 表示单个 SST 解析后的表级元信息。
// 这里缓存 footer、bloom、key 范围和稀疏索引，但不持有打开中的文件句柄。
type TableMeta struct {
	mu               sync.Mutex
	path             string
	indexStartOffset uint64
	minKey           string
	maxKey           string
	bloom            *bloom
	index            []indexEntry
	indexLoaded      bool
	indexErr         error
}

func openTableMeta(path string) (*TableMeta, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, 64*1024)

	// 1) 先校验 header，确认这是合法的 SST 文件。
	var m uint32
	if err := binary.Read(r, binary.LittleEndian, &m); err != nil {
		return nil, err
	}
	if m != magic {
		return nil, ErrCorruptSST
	}

	// 2) 读 count，迭代器依赖它判断 record 数量，这里也顺带完成格式校验。
	var count uint32
	if err := binary.Read(r, binary.LittleEndian, &count); err != nil {
		return nil, ErrCorruptSST
	}
	_ = count

	// 3) footer 里保存了 index、bloom、meta 三个区域的边界偏移。
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := st.Size()

	indexStartOffset, bloomStartOffset, metaStartOffset, err := loadFooter(f, fileSize)
	if err != nil {
		return nil, err
	}

	// 4) bloom 是热点元信息，初始化时就直接读入内存。
	br := io.NewSectionReader(f, int64(bloomStartOffset), int64(metaStartOffset-bloomStartOffset))
	bloomBytes, err := io.ReadAll(br)
	if err != nil {
		return nil, err
	}

	bf, ok := unmarshalBloom(bloomBytes)
	if !ok || bf.m == 0 || bf.k == 0 {
		return nil, ErrCorruptSST
	}

	// 5) 读取表级 key 范围，避免 DB 层再通过全表扫描求 min/max。
	minKey, maxKey, err := loadBounds(f, fileSize)
	if err != nil {
		return nil, err
	}

	return &TableMeta{
		path:             path,
		indexStartOffset: indexStartOffset,
		minKey:           minKey,
		maxKey:           maxKey,
		bloom:            bf,
	}, nil
}

func (tm *TableMeta) Get(key string) ([]byte, GetResult, error) {
	// bloom 明确判定不存在时，直接返回，不去碰索引和数据区。
	if !tm.bloom.mayContain(key) {
		return nil, NotFound, nil
	}

	// 只有“可能存在”时才加载稀疏索引，保住 miss 的快路径。
	if err := tm.ensureIndex(); err != nil {
		return nil, NotFound, err
	}

	// 元信息常驻内存，但真正的数据区仍然按次打开文件读取。
	f, err := os.Open(tm.path)
	if err != nil {
		return nil, NotFound, err
	}
	defer f.Close()

	// 稀疏索引只负责缩小扫描区间，不直接返回最终值。
	start, end := pickScanRange(tm.index, tm.indexStartOffset, key)
	if end <= start {
		return nil, NotFound, ErrCorruptSST
	}

	section := io.NewSectionReader(f, int64(start), int64(end-start))
	sr := bufio.NewReaderSize(section, 64*1024)

	for {
		var keyLen uint32
		var valLen uint32

		if err := binary.Read(sr, binary.LittleEndian, &keyLen); err != nil {
			// 扫描区间读完还没找到，就说明不存在。
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil, NotFound, nil
			}
			return nil, NotFound, ErrCorruptSST
		}
		if err := binary.Read(sr, binary.LittleEndian, &valLen); err != nil {
			return nil, NotFound, ErrCorruptSST
		}

		tomb, err := sr.ReadByte()
		if err != nil {
			return nil, NotFound, ErrCorruptSST
		}

		keyB := make([]byte, keyLen)
		if _, err := io.ReadFull(sr, keyB); err != nil {
			return nil, NotFound, ErrCorruptSST
		}

		var valB []byte
		if valLen > 0 {
			valB = make([]byte, valLen)
			if _, err := io.ReadFull(sr, valB); err != nil {
				return nil, NotFound, ErrCorruptSST
			}
		}

		k := string(keyB)
		if k == key {
			// tombstone 在 SST 中也必须优先解释为“已删除”。
			if tomb == 1 {
				return nil, Deleted, nil
			}
			return valB, Found, nil
		}
		if k > key {
			// SST 内部按 key 有序；一旦超过目标 key，就可以提前结束。
			return nil, NotFound, nil
		}
	}
}

func (tm *TableMeta) ensureIndex() error {
	tm.mu.Lock()
	if tm.indexLoaded {
		err := tm.indexErr
		tm.mu.Unlock()
		return err
	}
	tm.mu.Unlock()

	// 真正的索引加载放在锁外，避免慢 IO 长时间阻塞别的查询。
	f, err := os.Open(tm.path)
	if err != nil {
		return err
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return err
	}

	// 这里会完整校验索引区格式；如果损坏，错误会被缓存下来。
	index, indexStartOffset2, err := loadIndex(f, st.Size())
	if err == nil && indexStartOffset2 != tm.indexStartOffset {
		err = ErrCorruptSST
	}

	tm.mu.Lock()
	defer tm.mu.Unlock()
	if !tm.indexLoaded {
		// 只有第一次真正加载时才写入缓存，避免并发覆盖。
		tm.index = index
		tm.indexErr = err
		tm.indexLoaded = true
	}
	return tm.indexErr
}

// TableCache 缓存 SST 的表级元信息。
// 这里只缓存 footer、bloom、key 范围和稀疏索引，不缓存文件句柄，
// 这样可以减少重复解析开销，又不会把文件删除语义变复杂。
type TableCache struct {
	mu     sync.Mutex
	tables map[string]*TableMeta
}

func NewTableCache() *TableCache {
	return &TableCache{tables: make(map[string]*TableMeta)}
}

func (c *TableCache) Get(path string, key string) ([]byte, GetResult, error) {
	meta, err := c.getMeta(path)
	if err != nil {
		return nil, NotFound, err
	}
	return meta.Get(key)
}

func (c *TableCache) Evict(path string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	delete(c.tables, path)
	c.mu.Unlock()
}

func (c *TableCache) Close() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	c.tables = make(map[string]*TableMeta)
	c.mu.Unlock()
	return nil
}

func (c *TableCache) Len() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.tables)
}

func (c *TableCache) getMeta(path string) (*TableMeta, error) {
	c.mu.Lock()
	if meta, ok := c.tables[path]; ok {
		c.mu.Unlock()
		return meta, nil
	}
	c.mu.Unlock()

	// 未命中时在锁外解析，避免多个表的慢路径互相阻塞。
	meta, err := openTableMeta(path)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	if existing, ok := c.tables[path]; ok {
		// 这里处理并发竞态：如果别的 goroutine 已经放进来了，复用已有缓存。
		c.mu.Unlock()
		return existing, nil
	}
	c.tables[path] = meta
	c.mu.Unlock()
	return meta, nil
}
