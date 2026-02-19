package db

import (
	"testing"
)

func TestDB_CompactFull_CorrectnessAndReopen(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	// 000001: A=1
	if err := d.Put("A", []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}

	// 000002: B=2
	if err := d.Put("B", []byte("2")); err != nil {
		t.Fatal(err)
	}
	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}

	// 000003: Delete(A)
	if err := d.Delete("A"); err != nil {
		t.Fatal(err)
	}
	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}

	if len(d.sstables) != 3 {
		t.Fatalf("want 3 sstables before compaction, got %d", len(d.sstables))
	}

	// compaction
	if err := d.CompactFull(); err != nil {
		t.Fatal(err)
	}

	if len(d.sstables) != 1 {
		t.Fatalf("want 1 sstable after compaction, got %d", len(d.sstables))
	}

	// A 应该仍然删除（tombstone 不复活）
	if _, ok, err := d.Get("A"); err != nil || ok {
		t.Fatalf("A should be deleted, ok=%v err=%v", ok, err)
	}

	// B 应该仍然存在
	v, ok, err := d.Get("B")
	if err != nil {
		t.Fatalf("get B err=%v", err)
	}
	if !ok || string(v) != "2" {
		t.Fatalf("B should be 2, ok=%v v=%q", ok, string(v))
	}

	_ = d.Close()

	// reopen 后再验证一次
	d2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()

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
}

func TestDB_CompactFull_KeepsNewestValue(t *testing.T) {
	dir := t.TempDir()
	d, _ := Open(dir)

	_ = d.Put("A", []byte("1"))
	_ = d.Flush()
	_ = d.Put("A", []byte("2"))
	_ = d.Flush()
	_ = d.Put("A", []byte("3"))
	_ = d.Flush()

	if err := d.CompactFull(); err != nil {
		t.Fatal(err)
	}

	v, ok, err := d.Get("A")
	if err != nil || !ok || string(v) != "3" {
		t.Fatalf("want A=3, ok=%v v=%q err=%v", ok, string(v), err)
	}
}
