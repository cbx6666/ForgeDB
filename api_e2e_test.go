package monolithdb_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"monolithdb"
)

func TestPublicAPI_EndToEndCreatesFiles(t *testing.T) {
	dir := t.TempDir()

	opt := monolithdb.DefaultOptions().
		WithWALSync(monolithdb.WALSyncEveryBatch, 0).
		WithAutoFlushBytes(1 << 20)

	d, err := monolithdb.OpenWithOptions(dir, opt)
	if err != nil {
		t.Fatal(err)
	}

	if err := d.Put("alpha", []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := d.Put("beta", []byte("2")); err != nil {
		t.Fatal(err)
	}
	if err := d.Delete("beta"); err != nil {
		t.Fatal(err)
	}

	var batch monolithdb.WriteBatch
	batch.Put("gamma", []byte("3"))
	batch.Put("delta", []byte("4"))
	if err := d.Write(&batch); err != nil {
		t.Fatal(err)
	}

	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}

	entries, err := d.Scan("a", "z")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 {
		t.Fatalf("want 3 visible entries after flush, got %d", len(entries))
	}

	it, err := d.NewIterator("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = it.Close() }()
	if !it.Valid() || it.Entry().Key != "alpha" {
		t.Fatalf("expected iterator to start at alpha, got %+v", it.Entry())
	}

	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	assertExists(t, filepath.Join(dir, "MANIFEST"))
	assertExists(t, filepath.Join(dir, "forge.wal"))

	l0Files, err := filepath.Glob(filepath.Join(dir, "sst", "l0", "*.sst"))
	if err != nil {
		t.Fatal(err)
	}
	if len(l0Files) == 0 {
		t.Fatal("expected at least one L0 sst file after flush")
	}
	for _, p := range l0Files {
		assertNonEmpty(t, p)
	}

	d2, err := monolithdb.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d2.Close() }()

	v, ok, err := d2.Get("alpha")
	if err != nil || !ok || string(v) != "1" {
		t.Fatalf("want alpha=1 after reopen, ok=%v v=%q err=%v", ok, string(v), err)
	}
	if _, ok, err := d2.Get("beta"); err != nil || ok {
		t.Fatalf("beta should be deleted after reopen, ok=%v err=%v", ok, err)
	}
}

func TestPublicAPI_RequestCompactionEventuallyCreatesHigherLevelFiles(t *testing.T) {
	dir := t.TempDir()

	d, err := monolithdb.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()

	for i, key := range []string{"a", "b", "c", "d", "e"} {
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

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		l1Files, err := filepath.Glob(filepath.Join(dir, "sst", "l1", "*.sst"))
		if err != nil {
			t.Fatal(err)
		}
		if len(l1Files) > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("expected compaction to eventually create files in sst/l1")
}

func assertExists(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected file exists: %s, err=%v", path, err)
	}
}

func assertNonEmpty(t *testing.T, path string) {
	t.Helper()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	if st.Size() == 0 {
		t.Fatalf("expected non-empty file: %s", path)
	}
}
