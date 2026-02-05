package sstable

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"monolithdb/internal/types"
)

func TestSSTableWriteAndGet(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "000001.sst")

	entries := []types.Entry{
		{Key: "a", Value: []byte("1"), Tombstone: false},
		{Key: "b", Value: []byte("hello"), Tombstone: false},
		{Key: "c", Value: nil, Tombstone: true},
	}

	if err := WriteTable(path, entries); err != nil {
		t.Fatal(err)
	}

	// a 命中
	v, ok, err := Get(path, "a")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || !bytes.Equal(v, []byte("1")) {
		t.Fatalf("expected a=1, got ok=%v v=%q", ok, v)
	}

	// b 命中
	v, ok, err = Get(path, "b")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || !bytes.Equal(v, []byte("hello")) {
		t.Fatalf("expected b=hello, got ok=%v v=%q", ok, v)
	}

	// c 是 tombstone，应该不存在
	v, ok, err = Get(path, "c")
	if err != nil {
		t.Fatal(err)
	}
	if ok || v != nil {
		t.Fatalf("expected c to be deleted, got ok=%v v=%v", ok, v)
	}

	// 不存在的 key
	v, ok, err = Get(path, "z")
	if err != nil {
		t.Fatal(err)
	}
	if ok || v != nil {
		t.Fatalf("expected z to be not found, got ok=%v v=%v", ok, v)
	}
}

func TestSSTableMagicMismatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.sst")

	// 手工写一个错误 magic 的文件头
	// header: [magic(uint32)][count(uint32)]
	bad := []byte{
		0x00, 0x00, 0x00, 0x00, // magic = 0
		0x00, 0x00, 0x00, 0x00, // count = 0
	}
	if err := os.WriteFile(path, bad, 0o644); err != nil {
		t.Fatal(err)
	}

	_, _, err := Get(path, "a")
	if err == nil {
		t.Fatalf("expected ErrCorruptSST, got nil")
	}
	if err != ErrCorruptSST {
		t.Fatalf("expected ErrCorruptSST, got %v", err)
	}
}
