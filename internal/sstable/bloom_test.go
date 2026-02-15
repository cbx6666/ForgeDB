package sstable

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"monolithdb/internal/types"
)

// 证明：Get 会先用 Bloom 做 negative lookup，直接返回 NotFound，
// 不会因为索引区损坏而报 ErrCorruptSST。
func TestBloomSkipsCorruptIndexOnMiss(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "000001.sst")

	// 多写一些 key，确保有索引区 + bloom 区
	n := indexStride*4 + 10
	entries := make([]types.Entry, 0, n)
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("k%06d", i)
		v := []byte(fmt.Sprintf("v%06d", i))
		entries = append(entries, types.Entry{Key: k, Value: v})
	}

	if err := WriteTable(path, entries); err != nil {
		t.Fatal(err)
	}

	// 打开文件，读取 footer，拿到 index/bloom 的 offset
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	st, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	fileSize := st.Size()

	indexStartOffset, bloomStartOffset, err := loadFooter(f, fileSize)
	if err != nil {
		t.Fatal(err)
	}

	footerStart := uint64(fileSize) - uint64(footerSize)

	// 读取 bloom 区，反序列化
	br := io.NewSectionReader(f, int64(bloomStartOffset), int64(footerStart-bloomStartOffset))
	bloomBytes, err := io.ReadAll(br)
	if err != nil {
		t.Fatal(err)
	}

	bf, ok := unmarshalBloom(bloomBytes)
	if !ok || bf.m == 0 || bf.k == 0 {
		t.Fatalf("failed to unmarshal bloom")
	}

	// 找一个 Bloom 明确判定“不存在”的 key，避免极低概率误判导致测试不稳定
	missKey := ""
	for i := 0; i < 100000; i++ {
		cand := fmt.Sprintf("absent-%d", i)
		if !bf.mayContain(cand) {
			missKey = cand
			break
		}
	}
	if missKey == "" {
		t.Fatalf("could not find a bloom-negative key; bloom parameters may be too small")
	}

	// 故意破坏索引区：[indexStartOffset, bloomStartOffset)
	// 如果 Get 在 miss 时仍去读索引，这里会触发 ErrCorruptSST
	if bloomStartOffset <= indexStartOffset {
		t.Fatalf("bad offsets: indexStart=%d bloomStart=%d", indexStartOffset, bloomStartOffset)
	}

	// 破坏一小段（足够把 indexCount / keyLen 搞坏）
	corruptLen := int64(32)
	if int64(bloomStartOffset-indexStartOffset) < corruptLen {
		corruptLen = int64(bloomStartOffset - indexStartOffset)
	}
	if corruptLen <= 0 {
		t.Fatalf("index section too small to corrupt")
	}

	junk := make([]byte, corruptLen)
	for i := range junk {
		junk[i] = 0xFF
	}

	if _, err := f.WriteAt(junk, int64(indexStartOffset)); err != nil {
		t.Fatal(err)
	}

	// 关闭写句柄，避免平台缓存差异
	_ = f.Close()

	// 关键断言：missKey Bloom negative => Get 应该直接 NotFound，不应因为索引坏掉报错
	v, res, err := Get(path, missKey)
	if err != nil {
		t.Fatalf("expected bloom miss to return nil error, got %v", err)
	}
	if res != NotFound || v != nil {
		t.Fatalf("expected NotFound with nil value, got res=%v v=%v", res, v)
	}
}
