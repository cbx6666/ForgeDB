package db

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"
)

// NeverSync 模式下，批量写路径不应触发 fsync。
func TestDBWALSyncNever_DoesNotCallSync(t *testing.T) {
	dir := t.TempDir()
	dbDir := filepath.Join(dir, "data")

	opt := defaultOptions()
	opt.walSync = WALSyncNever

	d, err := OpenWithOptions(dbDir, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()

	var mu sync.Mutex
	syncCalls := 0
	d.syncWAL = func() error {
		mu.Lock()
		defer mu.Unlock()
		syncCalls++
		return nil
	}

	if err := d.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if syncCalls != 0 {
		t.Fatalf("expected sync not called, got %d", syncCalls)
	}
}

// SyncEveryBatch 模式下，每个成功提交的批次都应触发一次 fsync。
func TestDBWALSyncEveryBatch_CallsSync(t *testing.T) {
	dir := t.TempDir()
	dbDir := filepath.Join(dir, "data")

	opt := defaultOptions()
	opt.walSync = WALSyncEveryBatch

	d, err := OpenWithOptions(dbDir, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()

	var mu sync.Mutex
	syncCalls := 0
	d.syncWAL = func() error {
		mu.Lock()
		defer mu.Unlock()
		syncCalls++
		return nil
	}

	if err := d.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if syncCalls != 1 {
		t.Fatalf("expected sync called once, got %d", syncCalls)
	}
}

// 如果 fsync 失败，这一批写不能进入 memtable，后续读取也应看到后台错误。
func TestDBWALSyncError_DoesNotApplyMemtable(t *testing.T) {
	dir := t.TempDir()
	dbDir := filepath.Join(dir, "data")

	opt := defaultOptions()
	opt.walSync = WALSyncEveryBatch

	d, err := OpenWithOptions(dbDir, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()

	wantErr := errors.New("sync failed")
	d.syncWAL = func() error {
		return wantErr
	}

	err = d.Put("k", []byte("v"))
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected sync error %v, got %v", wantErr, err)
	}

	if _, ok, err := d.Get("k"); !errors.Is(err, wantErr) || ok {
		t.Fatalf("expected bg error and missing key, ok=%v err=%v", ok, err)
	}
}
