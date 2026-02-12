package sstable

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
	"sort"
)

const (
	// 稀疏索引间隔：每隔多少条 record 写一个索引项
	indexStride = 32

	// footer 只存一个 uint64 的 indexStartOffset（8 bytes）
	footerSize = 8

	// 简单的防爆上限（防止坏文件造成 OOM）
	maxIndexKeySize = 1 << 20 // 1MB
	maxIndexCount   = 1 << 20 // 约 100 万条索引项，上限很宽

	headerSize = 8 // magic(uint32) + count(uint32)
)

type indexEntry struct {
	key    string
	offset uint64
}

// loadIndex 尝试从文件尾部加载索引。
// 返回：entries, indexOffset, err
func loadIndex(f *os.File, fileSize int64) ([]indexEntry, uint64, error) {
	// 至少得放下：header(8) + footer(8)
	if fileSize < int64(headerSize+footerSize) {
		return nil, 0, ErrCorruptSST
	}

	// 读 footer：文件最后 8 字节是 indexStartOffset
	if _, err := f.Seek(-footerSize, io.SeekEnd); err != nil {
		return nil, 0, err
	}

	var indexStartOffset uint64
	if err := binary.Read(f, binary.LittleEndian, &indexStartOffset); err != nil {
		return nil, 0, ErrCorruptSST
	}

	// 基本合法性：索引区必须在文件内，并且位于 footer 之前
	if indexStartOffset < uint64(headerSize) {
		return nil, 0, ErrCorruptSST
	}
	if int64(indexStartOffset) >= fileSize-int64(footerSize) {
		return nil, 0, ErrCorruptSST
	}

	// Seek 到索引区起点，读 indexCount
	if _, err := f.Seek(int64(indexStartOffset), io.SeekStart); err != nil {
		return nil, 0, err
	}

	r := bufio.NewReaderSize(f, 64*1024)

	var indexCount uint32
	if err := binary.Read(r, binary.LittleEndian, &indexCount); err != nil {
		return nil, 0, ErrCorruptSST
	}
	if indexCount == 0 || indexCount > maxIndexCount {
		return nil, 0, ErrCorruptSST
	}

	entries := make([]indexEntry, indexCount)
	for i := uint32(0); i < indexCount; i++ {
		// 读取 key
		var keyLen uint32
		if err := binary.Read(r, binary.LittleEndian, &keyLen); err != nil {
			return nil, 0, ErrCorruptSST
		}
		if keyLen == 0 || keyLen > maxIndexKeySize {
			return nil, 0, ErrCorruptSST
		}

		keyB := make([]byte, keyLen)
		if _, err := io.ReadFull(r, keyB); err != nil {
			return nil, 0, ErrCorruptSST
		}

		// 读取 offset
		var recordOffset uint64
		if err := binary.Read(r, binary.LittleEndian, &recordOffset); err != nil {
			return nil, 0, ErrCorruptSST
		}

		// recordOffset 必须指向数据区（严格小于 indexStartOffset）
		if recordOffset < uint64(headerSize) || recordOffset >= indexStartOffset {
			return nil, 0, ErrCorruptSST
		}

		entries[i] = indexEntry{key: string(keyB), offset: recordOffset}
	}

	// 索引必须按 key 递增
	if !sort.SliceIsSorted(entries, func(i, j int) bool { return entries[i].key < entries[j].key }) {
		return nil, 0, ErrCorruptSST
	}

	return entries, indexStartOffset, nil
}

// pickScanRange 根据 target key 选择扫描区间 [startOffset, endOffset)。
func pickScanRange(entries []indexEntry, indexOffset uint64, target string) (start uint64, end uint64) {
	// indexOffset 是索引区起点（数据区终点）
	end = indexOffset

	// 找到最后一个 <= target 的索引项
	i := sort.Search(len(entries), func(i int) bool { return entries[i].key > target }) - 1 // 返回最小的 i，使得 f(i) == true
	if i < 0 {
		i = 0
	}

	start = entries[i].offset

	if i+1 < len(entries) {
		next := entries[i+1].offset
		if next > start && next < end {
			end = next
		}
	}
	return start, end
}
