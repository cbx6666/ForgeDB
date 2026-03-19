package sstable

import (
	"bufio"
	"encoding/binary"
	"os"

	"monolithdb/internal/types"
)

const (
	magic      uint32 = 0x46534442 // 'FSDB'，用于识别 SSTable 文件
	headerSize uint32 = 8          // magic(uint32) + count(uint32)
)

type countWriter struct {
	w *bufio.Writer
	n uint64
}

func newCountWriter(f *os.File) *countWriter {
	return &countWriter{w: bufio.NewWriterSize(f, 64*1024)}
}

func (cw *countWriter) Write(p []byte) (int, error) {
	n, err := cw.w.Write(p)
	cw.n += uint64(n)
	return n, err
}

func (cw *countWriter) WriteByte(b byte) error {
	if err := cw.w.WriteByte(b); err != nil {
		return err
	}
	cw.n++
	return nil
}

func (cw *countWriter) Flush() error { return cw.w.Flush() }

// WriteTable 将有序的 entries 写入一个 SSTable 文件。
func WriteTable(path string, entries []types.Entry) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	w := newCountWriter(f)

	// 1) 写 header：magic + count
	if err := binary.Write(w, binary.LittleEndian, magic); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, uint32(len(entries))); err != nil {
		return err
	}

	bf := newBloom(1<<20, 7)

	// 2) 写 records 和索引
	var idx []indexEntry

	for i, e := range entries {
		recOff := w.n

		// 写索引
		if i%indexStride == 0 {
			idx = append(idx, indexEntry{key: e.Key, offset: recOff})
		}

		keyB := []byte(e.Key)
		valB := e.Value

		if err := binary.Write(w, binary.LittleEndian, uint32(len(keyB))); err != nil {
			return err
		}
		if err := binary.Write(w, binary.LittleEndian, uint32(len(valB))); err != nil {
			return err
		}

		var tomb byte = 0
		if e.Tombstone {
			tomb = 1
		}
		if err := w.WriteByte(tomb); err != nil {
			return err
		}

		if _, err := w.Write(keyB); err != nil {
			return err
		}
		if len(valB) > 0 {
			if _, err := w.Write(valB); err != nil {
				return err
			}
		}

		// 写入 bloom
		bf.add(e.Key)
	}

	// 写索引
	indexStartOffset := w.n

	// indexCount
	if err := binary.Write(w, binary.LittleEndian, uint32(len(idx))); err != nil {
		return err
	}

	// index entries: [keyLen][keyBytes][recordOffset(uint64)]
	for _, it := range idx {
		kb := []byte(it.key)
		if err := binary.Write(w, binary.LittleEndian, uint32(len(kb))); err != nil {
			return err
		}
		if _, err := w.Write(kb); err != nil {
			return err
		}
		if err := binary.Write(w, binary.LittleEndian, it.offset); err != nil {
			return err
		}
	}

	// 写 bloomStartOffset
	bloomStartOffset := w.n
	bloomBytes := bf.marshal()
	if _, err := w.Write(bloomBytes); err != nil {
		return err
	}

	// 写入表级 key 范围，供恢复和 compaction 直接读取。
	metaStartOffset := w.n
	minKey := ""
	maxKey := ""
	if len(entries) > 0 {
		minKey = entries[0].Key
		maxKey = entries[len(entries)-1].Key
	}
	minKeyBytes := []byte(minKey)
	maxKeyBytes := []byte(maxKey)
	if err := binary.Write(w, binary.LittleEndian, uint32(len(minKeyBytes))); err != nil {
		return err
	}
	if len(minKeyBytes) > 0 {
		if _, err := w.Write(minKeyBytes); err != nil {
			return err
		}
	}
	if err := binary.Write(w, binary.LittleEndian, uint32(len(maxKeyBytes))); err != nil {
		return err
	}
	if len(maxKeyBytes) > 0 {
		if _, err := w.Write(maxKeyBytes); err != nil {
			return err
		}
	}

	// footer
	if err := binary.Write(w, binary.LittleEndian, indexStartOffset); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, bloomStartOffset); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, metaStartOffset); err != nil {
		return err
	}

	if err := w.Flush(); err != nil {
		return err
	}
	return f.Sync()
}

// Get 查询指定 key。
// 当前实现会按需加载表元信息，再到对应的数据区间里做扫描。
func Get(path string, key string) ([]byte, GetResult, error) {
	meta, err := openTableMeta(path)
	if err != nil {
		return nil, NotFound, err
	}
	return meta.Get(key)
}
