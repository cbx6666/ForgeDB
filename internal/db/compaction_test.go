package db

import "testing"

func TestDB_CompactL0ToL1_CorrectnessAndReopen(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	// 000001: A=1
	if err := d.Put("A", []byte("1")); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}
	if err := d.Flush(); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}

	// 000002: B=2
	if err := d.Put("B", []byte("2")); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}
	if err := d.Flush(); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}

	// 000003: Delete(A)
	if err := d.Delete("A"); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}
	if err := d.Flush(); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}

	// 目前我们还没到阈值，应该都在 L0
	if len(d.l0) != 3 {
		_ = d.Close()
		t.Fatalf("want 3 l0 sstables before compaction, got %d", len(d.l0))
	}

	// 手动 compaction：L0 -> L1
	if err := d.CompactL0ToL1(); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}

	if len(d.l0) != 0 {
		_ = d.Close()
		t.Fatalf("want l0 empty after compaction, got %d", len(d.l0))
	}
	if len(d.l1) != 1 {
		_ = d.Close()
		t.Fatalf("want 1 l1 sstable after compaction, got %d", len(d.l1))
	}

	// A 应该仍然删除（tombstone 不复活）
	if _, ok, err := d.Get("A"); err != nil || ok {
		_ = d.Close()
		t.Fatalf("A should be deleted, ok=%v err=%v", ok, err)
	}

	// B 应该仍然存在
	v, ok, err := d.Get("B")
	if err != nil {
		_ = d.Close()
		t.Fatalf("get B err=%v", err)
	}
	if !ok || string(v) != "2" {
		_ = d.Close()
		t.Fatalf("B should be 2, ok=%v v=%q", ok, string(v))
	}

	// 关闭再 reopen（Windows 必须先关）
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	d2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d2.Close() }()

	// reopen 后再验证一次
	if _, ok, err := d2.Get("A"); err != nil || ok {
		t.Fatalf("A should be deleted after reopen, ok=%v err=%v", ok, err)
	}
	v2, ok, err := d2.Get("B")
	if err != nil {
		t.Fatalf("get B after reopen err=%v", err)
	}
	if !ok || string(v2) != "2" {
		t.Fatalf("B should be 2 after reopen, ok=%v v=%q", ok, string(v2))
	}

	// reopen 后，L0 可能为空，L1 应该至少有 1 个
	if len(d2.l1) != 1 {
		t.Fatalf("want 1 l1 sstable after reopen, got %d", len(d2.l1))
	}
}

func TestDB_CompactL0ToL1_KeepsNewestValue(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()

	_ = d.Put("A", []byte("1"))
	_ = d.Flush()
	_ = d.Put("A", []byte("2"))
	_ = d.Flush()
	_ = d.Put("A", []byte("3"))
	_ = d.Flush()

	// 手动 compaction：确保 A 的 newest 版本被保留
	if err := d.CompactL0ToL1(); err != nil {
		t.Fatal(err)
	}

	v, ok, err := d.Get("A")
	if err != nil || !ok || string(v) != "3" {
		t.Fatalf("want A=3, ok=%v v=%q err=%v", ok, string(v), err)
	}
}

func TestDB_FlushAutoCompaction_L0ToL1(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	// 连续 flush 多次，让 L0 达到阈值触发 compaction
	for i := 0; i < autoCompactThreshold; i++ {
		key := string(rune('A' + i))
		if err := d.Put(key, []byte("v")); err != nil {
			_ = d.Close()
			t.Fatal(err)
		}
		if err := d.Flush(); err != nil {
			_ = d.Close()
			t.Fatal(err)
		}
	}

	// 达到阈值后应触发 L0->L1：L0 清空，L1 变成 1 个
	if len(d.l0) != 0 || len(d.l1) != 1 {
		_ = d.Close()
		t.Fatalf("expected auto compaction to make L0 empty and L1=1, got L0=%d L1=%d", len(d.l0), len(d.l1))
	}

	// reopen 再验证（先关 d）
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	d2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d2.Close() }()

	// 数据仍可读
	for i := 0; i < autoCompactThreshold; i++ {
		key := string(rune('A' + i))
		v, ok, err := d2.Get(key)
		if err != nil || !ok || string(v) != "v" {
			t.Fatalf("key %s lost after auto compaction: ok=%v v=%q err=%v", key, ok, string(v), err)
		}
	}

	// reopen 后 L1 应该存在
	if len(d2.l1) != 1 {
		t.Fatalf("expected L1=1 after reopen, got %d", len(d2.l1))
	}
}
