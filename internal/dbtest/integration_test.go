package dbtest

import (
	"bytes"
	"path/filepath"
	"sync"
	"testing"

	db "monolithdb/internal/db"
)

// integration_test.go 验证公开 API 的黑盒集成行为。

// TestDBPutGet 验证基础的单键写入与读取。
func TestDBPutGet(t *testing.T) {
	dir := t.TempDir()
	dbDir := filepath.Join(dir, "data")

	d, err := db.Open(dbDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()

	if err := d.Put("a", []byte("1")); err != nil {
		t.Fatal(err)
	}

	v, ok, err := d.Get("a")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("expected key a to exist")
	}
	if !bytes.Equal(v, []byte("1")) {
		t.Fatalf("expected value 1, got %q", v)
	}
}

// TestDBPutFlushReopen 验证显式 flush 后重启仍能读到数据。
func TestDBPutFlushReopen(t *testing.T) {
	dir := t.TempDir()
	dbDir := filepath.Join(dir, "data")

	d, err := db.Open(dbDir)
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

	d2, err := db.Open(dbDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d2.Close() }()

	v, ok, err := d2.Get("k")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("expected key k to exist after reopen")
	}
	if !bytes.Equal(v, []byte("v")) {
		t.Fatalf("expected value v, got %q", v)
	}
}

// 关键语义：删除写成 tombstone 后不能被旧 SST 覆盖
// TestDBDeleteTombstone 验证 tombstone 落盘后不会被旧 SST 覆盖。
func TestDBDeleteTombstone(t *testing.T) {
	dir := t.TempDir()
	dbDir := filepath.Join(dir, "data")

	d, err := db.Open(dbDir)
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

	d2, err := db.Open(dbDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d2.Close() }()

	v, ok, err := d2.Get("k")
	if err != nil {
		t.Fatal(err)
	}
	if ok || v != nil {
		t.Fatalf("expected key k to be deleted, got ok=%v v=%v", ok, v)
	}
}

// TestDBDeleteOverridesSST 验证 mem 中的 tombstone 会遮蔽旧 SST 值。
func TestDBDeleteOverridesSST(t *testing.T) {
	dir := t.TempDir()
	dbDir := filepath.Join(dir, "data")

	d, err := db.Open(dbDir)
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

	v, ok, err := d.Get("k")
	if err != nil {
		t.Fatal(err)
	}
	if ok || v != nil {
		t.Fatalf("expected mem tombstone to override SST value, got ok=%v v=%v", ok, v)
	}
}

// TestDBConcurrentPuts 验证并发 Put 之后所有键值都能正确读回。
func TestDBConcurrentPuts(t *testing.T) {
	dir := t.TempDir()
	dbDir := filepath.Join(dir, "data")

	d, err := db.Open(dbDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()

	const total = 64

	var wg sync.WaitGroup
	wg.Add(total)
	for i := 0; i < total; i++ {
		i := i
		go func() {
			defer wg.Done()
			key := makeTestKey(i)
			val := makeTestValue(i)
			if err := d.Put(key, val); err != nil {
				t.Errorf("put %s failed: %v", key, err)
			}
		}()
	}
	wg.Wait()

	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < total; i++ {
		key := makeTestKey(i)
		want := makeTestValue(i)
		got, ok, err := d.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		if !ok || !bytes.Equal(got, want) {
			t.Fatalf("want %s=%q, ok=%v got=%q", key, want, ok, got)
		}
	}
}

// TestDBWriteBatch_AppliesAllOperations 验证批量写会原子应用所有操作。
func TestDBWriteBatch_AppliesAllOperations(t *testing.T) {
	dir := t.TempDir()
	dbDir := filepath.Join(dir, "data")

	d, err := db.Open(dbDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()

	if err := d.Put("stale", []byte("old")); err != nil {
		t.Fatal(err)
	}

	var batch db.WriteBatch
	batch.Put("a", []byte("1"))
	batch.Put("b", []byte("2"))
	batch.Delete("stale")

	if err := d.Write(&batch); err != nil {
		t.Fatal(err)
	}

	va, ok, err := d.Get("a")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || !bytes.Equal(va, []byte("1")) {
		t.Fatalf("expected a=1, ok=%v got=%q", ok, va)
	}

	vb, ok, err := d.Get("b")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || !bytes.Equal(vb, []byte("2")) {
		t.Fatalf("expected b=2, ok=%v got=%q", ok, vb)
	}

	vstale, ok, err := d.Get("stale")
	if err != nil {
		t.Fatal(err)
	}
	if ok || vstale != nil {
		t.Fatalf("expected stale deleted, ok=%v v=%v", ok, vstale)
	}
}

// TestDBWriteBatch_FlushAndReopen 验证批量写在 flush 和重启后依然正确。
func TestDBWriteBatch_FlushAndReopen(t *testing.T) {
	dir := t.TempDir()
	dbDir := filepath.Join(dir, "data")

	d, err := db.Open(dbDir)
	if err != nil {
		t.Fatal(err)
	}

	var batch db.WriteBatch
	batch.Put("a", []byte("1"))
	batch.Put("b", []byte("2"))
	batch.Delete("missing")

	if err := d.Write(&batch); err != nil {
		t.Fatal(err)
	}
	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	d2, err := db.Open(dbDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d2.Close() }()

	for _, tc := range []struct {
		key  string
		want []byte
	}{
		{key: "a", want: []byte("1")},
		{key: "b", want: []byte("2")},
	} {
		got, ok, err := d2.Get(tc.key)
		if err != nil {
			t.Fatal(err)
		}
		if !ok || !bytes.Equal(got, tc.want) {
			t.Fatalf("expected %s=%q, ok=%v got=%q", tc.key, tc.want, ok, got)
		}
	}
}

// makeTestKey 生成并发测试使用的稳定键。
func makeTestKey(i int) string {
	return string([]byte{byte('a' + i%26), byte('0' + (i/26)%10), byte('0' + (i/260)%10)})
}

// makeTestValue 生成并发测试使用的稳定值。
func makeTestValue(i int) []byte {
	return []byte{byte('v'), byte('0' + i%10), byte('0' + (i/10)%10)}
}
