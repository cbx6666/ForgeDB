package sstable

import (
	"bytes"
	"path/filepath"
	"testing"

	"monolithdb/internal/types"
)

func TestTableCache_ReusesParsedMetadata(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "000001.sst")

	entries := []types.Entry{
		{Key: "a", Value: []byte("1")},
		{Key: "b", Value: []byte("2")},
	}
	if err := WriteTable(path, entries); err != nil {
		t.Fatal(err)
	}

	cache := NewTableCache()

	v1, res, err := cache.Get(path, "a")
	if err != nil || res != Found || !bytes.Equal(v1, []byte("1")) {
		t.Fatalf("first get failed: res=%v v=%q err=%v", res, v1, err)
	}
	if cache.Len() != 1 {
		t.Fatalf("expected one cached table, got %d", cache.Len())
	}

	v2, res, err := cache.Get(path, "b")
	if err != nil || res != Found || !bytes.Equal(v2, []byte("2")) {
		t.Fatalf("second get failed: res=%v v=%q err=%v", res, v2, err)
	}
	if cache.Len() != 1 {
		t.Fatalf("expected cache reuse, got %d tables", cache.Len())
	}
}

func TestTableCache_EvictReloadsTable(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "000001.sst")

	entries := []types.Entry{
		{Key: "a", Value: []byte("1")},
	}
	if err := WriteTable(path, entries); err != nil {
		t.Fatal(err)
	}

	cache := NewTableCache()
	if _, _, err := cache.Get(path, "a"); err != nil {
		t.Fatal(err)
	}
	cache.Evict(path)
	if cache.Len() != 0 {
		t.Fatalf("expected empty cache after evict, got %d", cache.Len())
	}

	v, res, err := cache.Get(path, "a")
	if err != nil || res != Found || !bytes.Equal(v, []byte("1")) {
		t.Fatalf("reload after evict failed: res=%v v=%q err=%v", res, v, err)
	}
}
