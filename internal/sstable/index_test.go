package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"monolithdb/internal/types"
)

// 这个测试证明：Get 使用索引缩小扫描范围
// 做法：写一个正常 SST -> 读取索引计算某个 key 的扫描区间 [start,end)
// 然后把 [0,start) 里的一段字节“故意破坏”
// 如果 Get 仍能成功返回，说明它没有全表扫，而是只扫了 [start,end)
func TestIndexLimitsScanRange(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "000001.sst")

	// 保证有多个索引段
	n := indexStride*4 + 10

	entries := make([]types.Entry, 0, n)
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("k%04d", i)
		v := []byte(fmt.Sprintf("v%04d", i))
		entries = append(entries, types.Entry{Key: k, Value: v})
	}

	if err := WriteTable(path, entries); err != nil {
		t.Fatal(err)
	}

	// 选一个不在第一段的 key（这样它的 startOffset 会明显大于 header）
	target := fmt.Sprintf("k%04d", indexStride*2+3)

	// 打开文件，加载索引并计算扫描区间
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}

	idx, indexStartOffset, err := loadIndex(f, st.Size())
	if err != nil {
		t.Fatal(err)
	}

	start, end := pickScanRange(idx, indexStartOffset, target)
	if !(start < end) {
		t.Fatalf("bad scan range: start=%d end=%d", start, end)
	}
	if start <= uint64(headerSize) {
		t.Fatalf("expected target start offset > header, got start=%d", start)
	}

	corruptLen := int64(16)
	corruptEnd := int64(start) // 必须严格小于 start 的区间，所以 end 用 start
	corruptPos := corruptEnd - corruptLen

	// 如果 start 太靠前，没法留出 16 字节，就缩短长度或直接 fail
	if corruptPos < int64(headerSize) {
		// 缩短破坏长度到能放下的最大值（至少 1）
		corruptPos = int64(headerSize)
		corruptLen = corruptEnd - corruptPos
		if corruptLen <= 0 {
			t.Fatalf("start too small to corrupt outside scan range: start=%d", start)
		}
	}

	junk := bytes.Repeat([]byte{0xFF}, int(corruptLen))
	if _, err := f.WriteAt(junk, corruptPos); err != nil {
		t.Fatal(err)
	}

	// 关闭写入句柄后再 Get（避免平台缓存差异）
	_ = f.Close()

	// 如果 Get 真走索引、只扫 [start,end)，应该仍然能找到 target
	v, res, err := Get(path, target)
	if err != nil {
		t.Fatalf("expected success, got err=%v", err)
	}
	if res != Found {
		t.Fatalf("expected Found, got %v", res)
	}
	want := []byte(fmt.Sprintf("v%04d", indexStride*2+3))
	if !bytes.Equal(v, want) {
		t.Fatalf("expected %q, got %q", want, v)
	}
}

// 反向验证：如果损坏发生在 target 的扫描区间内，Get 必须读到并报 ErrCorruptSST
func TestIndexCorruptionInsideRange(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "000001.sst")

	n := indexStride*3 + 10
	entries := make([]types.Entry, 0, n)
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("k%04d", i)
		v := []byte(fmt.Sprintf("v%04d", i))
		entries = append(entries, types.Entry{Key: k, Value: v})
	}
	if err := WriteTable(path, entries); err != nil {
		t.Fatal(err)
	}

	// 选一个在第二段附近的 key（确保 start 不太小，容易破坏到区间内部）
	targetIdx := indexStride + 6
	target := fmt.Sprintf("k%04d", targetIdx)

	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}

	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		t.Fatal(err)
	}

	idx, indexStartOffset, err := loadIndex(f, st.Size())
	if err != nil {
		_ = f.Close()
		t.Fatal(err)
	}

	start, end := pickScanRange(idx, indexStartOffset, target)
	if !(start < end) {
		_ = f.Close()
		t.Fatalf("bad scan range: start=%d end=%d", start, end)
	}

	// 破坏区间放在 [start, min(start+16, end))
	corruptPos := int64(start)
	corruptLen := int64(16)
	if int64(end)-corruptPos < corruptLen {
		corruptLen = int64(end) - corruptPos
	}
	if corruptLen <= 0 {
		_ = f.Close()
		t.Fatalf("range too small to corrupt: start=%d end=%d", start, end)
	}

	junk := bytes.Repeat([]byte{0xEE}, int(corruptLen))
	if _, err := f.WriteAt(junk, corruptPos); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	_ = f.Close()

	_, _, err = Get(path, target)
	if err == nil {
		t.Fatalf("expected ErrCorruptSST, got nil")
	}
	if err != ErrCorruptSST {
		t.Fatalf("expected ErrCorruptSST, got %v", err)
	}
}

// 索引必须存在：footer 越界（indexStartOffset 指向文件外）应判为 ErrCorruptSST
func TestIndexFooterOutOfRange(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "000001.sst")

	entries := []types.Entry{
		{Key: "a", Value: []byte("1")},
		{Key: "b", Value: []byte("2")},
		{Key: "c", Value: []byte("3")},
	}
	if err := WriteTable(path, entries); err != nil {
		t.Fatal(err)
	}

	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}

	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if st.Size() < footerSize {
		_ = f.Close()
		t.Fatalf("file too small unexpectedly: %d", st.Size())
	}

	// 把 footer 里的 indexStartOffset 改成一个明显越界的大值
	huge := uint64(1 << 60)
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, huge)

	if _, err := f.WriteAt(buf, st.Size()-footerSize); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	_ = f.Close()

	_, _, err = Get(path, "b")
	if err == nil {
		t.Fatalf("expected ErrCorruptSST, got nil")
	}
	if err != ErrCorruptSST {
		t.Fatalf("expected ErrCorruptSST, got %v", err)
	}
}
