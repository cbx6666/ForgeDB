package db

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// db_write_sync_test.go 验证 WAL 同步策略和后台同步协作语义。

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

// SyncInterval 模式下，fsync 应在定时器到点后异步触发，而不是在前台写路径里立刻执行。
func TestDBWALSyncInterval_CallsSyncAfterInterval(t *testing.T) {
	dir := t.TempDir()
	dbDir := filepath.Join(dir, "data")

	opt := defaultOptions()
	opt.walSync = WALSyncInterval
	opt.walSyncInterval = 40 * time.Millisecond

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

	time.Sleep(10 * time.Millisecond)
	mu.Lock()
	if syncCalls != 0 {
		mu.Unlock()
		t.Fatalf("expected sync not called immediately, got %d", syncCalls)
	}
	mu.Unlock()

	waitForCondition(t, 300*time.Millisecond, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return syncCalls >= 1
	}, "expected interval sync to be triggered")
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

// Interval 模式下，fsync 异步失败后应最终暴露为后台错误。
func TestDBWALSyncInterval_ErrorEventuallySetsBGErr(t *testing.T) {
	dir := t.TempDir()
	dbDir := filepath.Join(dir, "data")

	opt := defaultOptions()
	opt.walSync = WALSyncInterval
	opt.walSyncInterval = 20 * time.Millisecond

	d, err := OpenWithOptions(dbDir, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()

	wantErr := errors.New("interval sync failed")
	d.syncWAL = func() error {
		return wantErr
	}

	if err := d.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}

	waitForCondition(t, 300*time.Millisecond, func() bool {
		_, _, err := d.Get("k")
		return errors.Is(err, wantErr)
	}, "expected interval sync error to surface through bgErr")
}

func waitForCondition(t *testing.T, deadline time.Duration, cond func() bool, onTimeout string) {
	t.Helper()

	start := time.Now()
	for {
		if cond() {
			return
		}
		if time.Since(start) >= deadline {
			t.Fatal(onTimeout)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
