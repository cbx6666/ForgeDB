package db

import (
	"bytes"
	"path/filepath"
	"testing"
)

func TestDBPutGet(t *testing.T) {
	dir := t.TempDir()
	dbDir := filepath.Join(dir, "data")

	d, err := Open(dbDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()

	if err := d.Put("a", []byte("1")); err != nil {
		t.Fatal(err)
	}

	v, ok := d.Get("a")
	if !ok {
		t.Fatalf("expected key a to exist")
	}
	if !bytes.Equal(v, []byte("1")) {
		t.Fatalf("expected value 1, got %q", v)
	}
}

func TestDBPutFlushReopen(t *testing.T) {
	dir := t.TempDir()
	dbDir := filepath.Join(dir, "data")

	d, err := Open(dbDir)
	if err != nil {
		t.Fatal(err)
	}

	if err := d.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	d2, err := Open(dbDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d2.Close() }()

	v, ok := d2.Get("k")
	if !ok {
		t.Fatalf("expected key k to exist after reopen")
	}
	if !bytes.Equal(v, []byte("v")) {
		t.Fatalf("expected value v, got %q", v)
	}
}

// 关键语义：删除写成 tombstone 后不能被旧 SST 覆盖
func TestDBDeleteTombstone(t *testing.T) {
	dir := t.TempDir()
	dbDir := filepath.Join(dir, "data")

	d, err := Open(dbDir)
	if err != nil {
		t.Fatal(err)
	}

	// 旧值落盘
	if err := d.Put("k", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}

	// 删除写 tombstone，再落盘
	if err := d.Delete("k"); err != nil {
		t.Fatal(err)
	}
	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}

	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	d2, err := Open(dbDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d2.Close() }()

	v, ok := d2.Get("k")
	if ok || v != nil {
		t.Fatalf("expected key k to be deleted, got ok=%v v=%v", ok, v)
	}
}

func TestDBDeleteOverridesSST(t *testing.T) {
	dir := t.TempDir()
	dbDir := filepath.Join(dir, "data")

	d, err := Open(dbDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()

	// 先让旧值进入 SST
	if err := d.Put("k", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}

	// 再在 mem 中 delete（不 flush）
	if err := d.Delete("k"); err != nil {
		t.Fatal(err)
	}

	v, ok := d.Get("k")
	if ok || v != nil {
		t.Fatalf("expected mem tombstone to override SST value, got ok=%v v=%v", ok, v)
	}
}
