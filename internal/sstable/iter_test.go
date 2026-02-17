package sstable

import (
	"fmt"
	"path/filepath"
	"testing"

	"monolithdb/internal/types"
)

func TestIter_ScanAllRecordsInOrder(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "000001.sst")

	// 写足够多的数据，覆盖 tombstone、普通 value
	n := indexStride*2 + 10
	entries := make([]types.Entry, 0, n)
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("k%06d", i)
		if i%17 == 0 {
			entries = append(entries, types.Entry{Key: k, Tombstone: true})
		} else {
			v := []byte(fmt.Sprintf("v%06d", i))
			entries = append(entries, types.Entry{Key: k, Value: v})
		}
	}

	if err := WriteTable(path, entries); err != nil {
		t.Fatal(err)
	}

	it, err := NewIter(path)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()

	// 逐条扫，证明：数量一致 + key 递增 + tombstone/value 语义正确
	gotCount := 0
	lastKey := ""

	for it.Valid() {
		e := it.Entry()

		if lastKey != "" && e.Key <= lastKey {
			t.Fatalf("keys not strictly increasing: last=%q cur=%q", lastKey, e.Key)
		}
		lastKey = e.Key

		var i int
		_, _ = fmt.Sscanf(e.Key, "k%06d", &i)

		if i%17 == 0 {
			if !e.Tombstone {
				t.Fatalf("expected tombstone at %q", e.Key)
			}
			if len(e.Value) != 0 {
				t.Fatalf("expected empty value for tombstone at %q, got %q", e.Key, string(e.Value))
			}
		} else {
			if e.Tombstone {
				t.Fatalf("unexpected tombstone at %q", e.Key)
			}
			want := fmt.Sprintf("v%06d", i)
			if string(e.Value) != want {
				t.Fatalf("value mismatch at %q: want %q got %q", e.Key, want, string(e.Value))
			}
		}

		gotCount++
		if err := it.Next(); err != nil {
			t.Fatal(err)
		}
	}

	if gotCount != len(entries) {
		t.Fatalf("record count mismatch: got=%d want=%d", gotCount, len(entries))
	}
}

func TestIter_EmptyTable(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "000001.sst")

	if err := WriteTable(path, nil); err != nil {
		t.Fatal(err)
	}

	it, err := NewIter(path)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()

	if it.Valid() {
		t.Fatalf("expected invalid iterator for empty table")
	}
}
