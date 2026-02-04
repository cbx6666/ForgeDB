package wal

import (
	"bytes"
	"path/filepath"
	"testing"
)

func TestWALAppendAndReplay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "forge.wal")

	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	// 写入两条 put + 一条 delete
	if err := w.AppendPut("a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := w.AppendPut("b", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := w.AppendDelete("a"); err != nil {
		t.Fatal(err)
	}

	records, err := Replay(path)
	if err != nil {
		t.Fatal(err)
	}

	if len(records) != 3 {
		t.Fatalf("expected 3 records, got %d", len(records))
	}

	// record 0: put a=1
	if records[0].Op != opPut || records[0].Key != "a" || !bytes.Equal(records[0].Value, []byte("1")) {
		t.Fatalf("unexpected record[0]: %+v", records[0])
	}

	// record 1: put b=hello
	if records[1].Op != opPut || records[1].Key != "b" || !bytes.Equal(records[1].Value, []byte("hello")) {
		t.Fatalf("unexpected record[1]: %+v", records[1])
	}

	// record 2: delete a
	if records[2].Op != opDelete || records[2].Key != "a" || len(records[2].Value) != 0 {
		t.Fatalf("unexpected record[2]: %+v", records[2])
	}
}
