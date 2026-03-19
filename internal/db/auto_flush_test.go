package db

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestDBAutoFlush_TriggersWithoutExplicitFlush(t *testing.T) {
	dir := t.TempDir()

	opt := DefaultOptions().WithAutoFlushBytes(32)
	d, err := OpenWithOptions(dir, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()

	want := strings.Repeat("v", 64)
	if err := d.Put("k1", []byte(want)); err != nil {
		t.Fatal(err)
	}

	waitUntil(
		t,
		2*time.Second,
		10*time.Millisecond,
		func() bool {
			s := snapshotLevels(d)
			return s.l0 > 0
		},
		func() string {
			s := snapshotLevels(d)
			return fmt.Sprintf("expected auto flush to create an L0 table, got L0=%d", s.l0)
		},
	)

	got, ok, err := d.Get("k1")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(got) != want {
		t.Fatalf("want k1 persisted by auto flush, ok=%v got=%q", ok, string(got))
	}
}

func TestDBAutoFlush_ForegroundWritesRemainVisible(t *testing.T) {
	dir := t.TempDir()

	opt := DefaultOptions().WithAutoFlushBytes(32)
	d, err := OpenWithOptions(dir, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()

	oldVal := strings.Repeat("a", 64)
	if err := d.Put("old", []byte(oldVal)); err != nil {
		t.Fatal(err)
	}

	waitUntil(
		t,
		2*time.Second,
		10*time.Millisecond,
		func() bool {
			s := snapshotLevels(d)
			return s.l0 > 0
		},
		func() string {
			s := snapshotLevels(d)
			return fmt.Sprintf("expected auto flush to create an L0 table, got L0=%d", s.l0)
		},
	)

	if err := d.Put("new", []byte("v-new")); err != nil {
		t.Fatal(err)
	}

	gotOld, okOld, err := d.Get("old")
	if err != nil {
		t.Fatal(err)
	}
	if !okOld || string(gotOld) != oldVal {
		t.Fatalf("want old key readable after auto flush, ok=%v got=%q", okOld, string(gotOld))
	}

	gotNew, okNew, err := d.Get("new")
	if err != nil {
		t.Fatal(err)
	}
	if !okNew || string(gotNew) != "v-new" {
		t.Fatalf("want new key readable after auto flush, ok=%v got=%q", okNew, string(gotNew))
	}
}
