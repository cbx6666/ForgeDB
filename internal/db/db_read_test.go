package db

import (
	"testing"
	"time"

	"monolithdb/internal/memtable"
)

// db_read_test.go 验证点查、范围读和多层来源合并语义。

func TestDBScan_MergesMemImmAndSST(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()

	if err := d.Put("a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := d.Put("b", []byte("2")); err != nil {
		t.Fatal(err)
	}
	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}

	d.mu.Lock()
	d.imm = memtable.NewMemTable()
	d.imm.Put("c", []byte("3"))
	d.imm.Delete("b")
	d.mem.Put("a", []byte("4"))
	d.mem.Put("d", []byte("5"))
	d.mu.Unlock()

	entries, err := d.Scan("a", "z")
	if err != nil {
		t.Fatal(err)
	}

	if len(entries) != 3 {
		t.Fatalf("want 3 entries, got %d", len(entries))
	}
	if entries[0].Key != "a" || string(entries[0].Value) != "4" {
		t.Fatalf("want a=4, got %+v", entries[0])
	}
	if entries[1].Key != "c" || string(entries[1].Value) != "3" {
		t.Fatalf("want c=3, got %+v", entries[1])
	}
	if entries[2].Key != "d" || string(entries[2].Value) != "5" {
		t.Fatalf("want d=5, got %+v", entries[2])
	}
}

func TestDBIterator_RespectsRangeBounds(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()

	if err := d.Put("a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}

	d.mu.Lock()
	d.imm = memtable.NewMemTable()
	d.imm.Put("c", []byte("3"))
	d.imm.Delete("b")
	d.mem.Put("d", []byte("4"))
	d.mu.Unlock()

	it, err := d.NewIterator("b", "d")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = it.Close() }()

	if !it.Valid() {
		t.Fatal("expected iterator to be valid")
	}
	ent := it.Entry()
	if ent.Key != "c" || string(ent.Value) != "3" {
		t.Fatalf("want c=3, got %+v", ent)
	}

	if err := it.Next(); err != nil {
		t.Fatal(err)
	}
	if it.Valid() {
		t.Fatalf("expected iterator to stop at end bound, got %+v", it.Entry())
	}
}

func TestDBScan_AfterCompactionIntoL1(t *testing.T) {
	dir := t.TempDir()

	opt := defaultOptions()
	opt.numLevels = 2
	opt.levels = []LevelOptions{
		{maxFiles: 2, pickN: 2, runMaxEntries: 0},
		{maxFiles: 0, pickN: 0, runMaxEntries: 64},
	}

	d, err := OpenWithOptions(dir, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()

	for i, key := range []string{"a", "b", "c"} {
		if err := d.Put(key, []byte{byte('1' + i)}); err != nil {
			t.Fatal(err)
		}
		if err := d.Flush(); err != nil {
			t.Fatal(err)
		}
	}

	if err := d.RequestCompaction(); err != nil {
		t.Fatal(err)
	}

	waitUntil(
		t,
		3*time.Second,
		10*time.Millisecond,
		func() bool {
			s := snapshotLevels(d)
			return s.l1 > 0
		},
		func() string {
			return "expected scan test data to reach L1"
		},
	)

	entries, err := d.Scan("", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 {
		t.Fatalf("want 3 entries after compaction, got %d", len(entries))
	}
	if entries[0].Key != "a" || string(entries[0].Value) != "1" {
		t.Fatalf("unexpected first entry: %+v", entries[0])
	}
	if entries[1].Key != "b" || string(entries[1].Value) != "2" {
		t.Fatalf("unexpected second entry: %+v", entries[1])
	}
	if entries[2].Key != "c" || string(entries[2].Value) != "3" {
		t.Fatalf("unexpected third entry: %+v", entries[2])
	}
}
