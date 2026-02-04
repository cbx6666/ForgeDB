package memtable

import (
	"bytes"
	"testing"
)

func TestMemTablePutGet(t *testing.T) {
	m := NewMemTable()

	m.Put("a", []byte("1"))
	v, ok := m.Get("a")
	if !ok {
		t.Fatalf("expected key a to exist")
	}
	if !bytes.Equal(v, []byte("1")) {
		t.Fatalf("expected value 1, got %q", v)
	}
}

func TestMemTablePutOverwrite(t *testing.T) {
	m := NewMemTable()

	m.Put("a", []byte("1"))
	m.Put("a", []byte("2"))

	v, ok := m.Get("a")
	if !ok {
		t.Fatalf("expected key a to exist")
	}
	if !bytes.Equal(v, []byte("2")) {
		t.Fatalf("expected value 2, got %q", v)
	}
}

func TestMemTableDelete(t *testing.T) {
	m := NewMemTable()

	m.Put("a", []byte("1"))
	m.Delete("a")

	_, ok := m.Get("a")
	if ok {
		t.Fatalf("expected key a to be deleted")
	}
}

func TestMemTableRange(t *testing.T) {
	m := NewMemTable()

	// 故意乱序插入，验证 Range 输出有序且边界正确
	m.Put("c", []byte("3"))
	m.Put("a", []byte("1"))
	m.Put("b", []byte("2"))
	m.Put("d", []byte("4"))

	// [b, d) 应该返回 b, c
	got := m.Range("b", "d")
	if len(got) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(got))
	}
	if got[0].Key != "b" || !bytes.Equal(got[0].Value, []byte("2")) {
		t.Fatalf("unexpected first entry: key=%s value=%q", got[0].Key, got[0].Value)
	}
	if got[1].Key != "c" || !bytes.Equal(got[1].Value, []byte("3")) {
		t.Fatalf("unexpected second entry: key=%s value=%q", got[1].Key, got[1].Value)
	}
}

func TestMemTableReturnsClonedBytes(t *testing.T) {
	m := NewMemTable()

	// Put 时传入的 slice 后续被修改，不应影响库内值
	buf := []byte("hello")
	m.Put("k", buf)
	buf[0] = 'X'

	v, ok := m.Get("k")
	if !ok {
		t.Fatalf("expected key k to exist")
	}
	if !bytes.Equal(v, []byte("hello")) {
		t.Fatalf("expected stored value to remain 'hello', got %q", v)
	}

	// Get 返回的 slice 被修改，不应影响库内值
	v[0] = 'Y'
	v2, ok := m.Get("k")
	if !ok {
		t.Fatalf("expected key k to exist")
	}
	if !bytes.Equal(v2, []byte("hello")) {
		t.Fatalf("expected stored value to remain 'hello' after modifying returned slice, got %q", v2)
	}
}
