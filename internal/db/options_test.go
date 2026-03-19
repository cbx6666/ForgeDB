package db

import (
	"testing"
	"time"
)

func TestDefaultOptions_UsesNeverSyncByDefault(t *testing.T) {
	opt := DefaultOptions()

	if opt.walSync != WALSyncNever {
		t.Fatalf("expected default walSync=%v, got %v", WALSyncNever, opt.walSync)
	}
	if opt.walSyncInterval <= 0 {
		t.Fatalf("expected positive default walSyncInterval, got %s", opt.walSyncInterval)
	}
}

func TestOptionsWithWALSync_ConfiguresIntervalMode(t *testing.T) {
	opt := DefaultOptions().WithWALSync(WALSyncInterval, 25*time.Millisecond)

	if opt.walSync != WALSyncInterval {
		t.Fatalf("expected walSync=%v, got %v", WALSyncInterval, opt.walSync)
	}
	if opt.walSyncInterval != 25*time.Millisecond {
		t.Fatalf("expected walSyncInterval=25ms, got %s", opt.walSyncInterval)
	}
}

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
