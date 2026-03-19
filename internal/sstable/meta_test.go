package sstable

import (
	"path/filepath"
	"testing"

	"monolithdb/internal/types"
)

func TestLoadBounds(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "000001.sst")

	entries := []types.Entry{
		{Key: "a", Value: []byte("1")},
		{Key: "m", Value: []byte("2")},
		{Key: "z", Value: []byte("3")},
	}
	if err := WriteTable(path, entries); err != nil {
		t.Fatal(err)
	}

	minKey, maxKey, err := LoadBounds(path)
	if err != nil {
		t.Fatal(err)
	}
	if minKey != "a" || maxKey != "z" {
		t.Fatalf("want bounds [a,z], got [%s,%s]", minKey, maxKey)
	}
}
