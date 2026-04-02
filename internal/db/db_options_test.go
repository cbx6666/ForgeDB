package db

import (
	"testing"
	"time"
)

// db_options_test.go 验证默认选项和链式配置接口的行为。

// TestDefaultOptions_UsesNeverSyncByDefault 验证默认配置采用 NeverSync。
func TestDefaultOptions_UsesNeverSyncByDefault(t *testing.T) {
	opt := DefaultOptions()

	if opt.walSync != WALSyncNever {
		t.Fatalf("expected default walSync=%v, got %v", WALSyncNever, opt.walSync)
	}
	if opt.walSyncInterval <= 0 {
		t.Fatalf("expected positive default walSyncInterval, got %s", opt.walSyncInterval)
	}
	if opt.maxImmutableMems <= 0 {
		t.Fatalf("expected positive default maxImmutableMems, got %d", opt.maxImmutableMems)
	}
}

// TestOptionsWithWALSync_ConfiguresIntervalMode 验证 interval 模式会记录给定同步周期。
func TestOptionsWithWALSync_ConfiguresIntervalMode(t *testing.T) {
	opt := DefaultOptions().WithWALSync(WALSyncInterval, 25*time.Millisecond)

	if opt.walSync != WALSyncInterval {
		t.Fatalf("expected walSync=%v, got %v", WALSyncInterval, opt.walSync)
	}
	if opt.walSyncInterval != 25*time.Millisecond {
		t.Fatalf("expected walSyncInterval=25ms, got %s", opt.walSyncInterval)
	}
}

// TestOptionsWithWALSync_KeepsExistingIntervalForNonIntervalMode 验证切到非 interval 模式时保留原周期。
func TestOptionsWithWALSync_KeepsExistingIntervalForNonIntervalMode(t *testing.T) {
	base := DefaultOptions().WithWALSync(WALSyncInterval, 25*time.Millisecond)
	opt := base.WithWALSync(WALSyncEveryBatch, time.Second)

	if opt.walSync != WALSyncEveryBatch {
		t.Fatalf("expected walSync=%v, got %v", WALSyncEveryBatch, opt.walSync)
	}
	if opt.walSyncInterval != 25*time.Millisecond {
		t.Fatalf("expected walSyncInterval unchanged, got %s", opt.walSyncInterval)
	}
}

// TestOptionsWithAutoFlushBytes_ConfiguresThreshold 验证自动 flush 阈值配置会生效。
func TestOptionsWithAutoFlushBytes_ConfiguresThreshold(t *testing.T) {
	opt := DefaultOptions().WithAutoFlushBytes(4096)

	if opt.autoFlushBytes != 4096 {
		t.Fatalf("expected autoFlushBytes=4096, got %d", opt.autoFlushBytes)
	}
}

// TestOptionsWithMaxImmutableMems_ConfiguresQueueLimit 验证 immutable queue 上限配置会生效。
func TestOptionsWithMaxImmutableMems_ConfiguresQueueLimit(t *testing.T) {
	opt := DefaultOptions().WithMaxImmutableMems(2)

	if opt.maxImmutableMems != 2 {
		t.Fatalf("expected maxImmutableMems=2, got %d", opt.maxImmutableMems)
	}
}
