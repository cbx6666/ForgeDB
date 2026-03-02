package manifest

import (
	"os"
	"path/filepath"
	"testing"
)

func countLinesByNewline(b []byte) int {
	// json.Encoder.Encode() 每条记录结尾会写 '\n'
	n := 0
	for _, c := range b {
		if c == '\n' {
			n++
		}
	}
	return n
}

func TestManifest_WriteAndLoadLastSnapshot(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "MANIFEST")

	m, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = m.Close() }()

	// write #1
	if err := m.WriteSnapshot(12, [][]string{
		{"1.sst", "2.sst"},
		{"3.sst"},
	}); err != nil {
		t.Fatalf("WriteSnapshot #1: %v", err)
	}

	// write #2
	wantNext := uint64(15)
	wantLevels := [][]string{
		{"5.sst"},
		{"3.sst", "6.sst"},
		{},
	}
	if err := m.WriteSnapshot(wantNext, wantLevels); err != nil {
		t.Fatalf("WriteSnapshot #2: %v", err)
	}

	last, err := LoadLastSnapshot(path)
	if err != nil {
		t.Fatalf("LoadLastSnapshot: %v", err)
	}
	if last.NextFile != wantNext {
		t.Fatalf("NextFile: got %d want %d", last.NextFile, wantNext)
	}
	if len(last.Levels) != len(wantLevels) {
		t.Fatalf("Levels len: got %d want %d", len(last.Levels), len(wantLevels))
	}
	for i := range wantLevels {
		if len(last.Levels[i]) != len(wantLevels[i]) {
			t.Fatalf("Levels[%d] len: got %d want %d", i, len(last.Levels[i]), len(wantLevels[i]))
		}
		for j := range wantLevels[i] {
			if last.Levels[i][j] != wantLevels[i][j] {
				t.Fatalf("Levels[%d][%d]: got %q want %q", i, j, last.Levels[i][j], wantLevels[i][j])
			}
		}
	}
}

func TestLoadLastSnapshot_TolerateCorruptTrailingLine(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "MANIFEST")

	m, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = m.Close() }()

	// last valid snapshot
	wantNext := uint64(7)
	wantLevels := [][]string{{"a.sst"}, {}}
	if err := m.WriteSnapshot(wantNext, wantLevels); err != nil {
		t.Fatalf("WriteSnapshot: %v", err)
	}
	_ = m.Close()

	// append a corrupt/incomplete line (simulate crash half-write)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("OpenFile append: %v", err)
	}
	_, _ = f.WriteString("{this is not json")
	_ = f.Close()

	last, err := LoadLastSnapshot(path)
	if err != nil {
		t.Fatalf("LoadLastSnapshot: %v", err)
	}
	if last.NextFile != wantNext {
		t.Fatalf("NextFile: got %d want %d", last.NextFile, wantNext)
	}
	if len(last.Levels) != len(wantLevels) || len(last.Levels[0]) != 1 || last.Levels[0][0] != "a.sst" {
		t.Fatalf("Levels: got %#v want %#v", last.Levels, wantLevels)
	}
}

func TestLoadLastSnapshot_NoSnapshot(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "MANIFEST")

	// file exists but has no valid snapshot lines
	if err := os.WriteFile(path, []byte("not json\n\n"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, err := LoadLastSnapshot(path)
	if err != ErrNoSnapshot {
		t.Fatalf("err: got %v want %v", err, ErrNoSnapshot)
	}
}

func TestManifest_CheckpointLatest_CompactsToOneLineAndCanAppend(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "MANIFEST")

	m, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = m.Close() }()

	// 写多条 snapshot
	if err := m.WriteSnapshot(1, [][]string{{"a.sst"}}); err != nil {
		t.Fatalf("WriteSnapshot #1: %v", err)
	}
	if err := m.WriteSnapshot(2, [][]string{{"b.sst"}, {"c.sst"}}); err != nil {
		t.Fatalf("WriteSnapshot #2: %v", err)
	}
	if err := m.WriteSnapshot(3, [][]string{{"d.sst"}}); err != nil {
		t.Fatalf("WriteSnapshot #3: %v", err)
	}

	// 确认 checkpoint 前确实 >= 3 行（避免测试“没测到”）
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile before: %v", err)
	}
	if got := countLinesByNewline(before); got < 3 {
		t.Fatalf("expected >= 3 lines before checkpoint, got %d", got)
	}

	// checkpoint：压成只剩最后一条
	if err := m.CheckpointLatest(); err != nil {
		t.Fatalf("CheckpointLatest: %v", err)
	}

	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile after: %v", err)
	}
	if got := countLinesByNewline(after); got != 1 {
		t.Fatalf("checkpoint did not compact: got %d lines, want 1", got)
	}

	// 还能继续追加（checkpoint 里必须重开文件）
	if err := m.WriteSnapshot(4, [][]string{{"e.sst"}}); err != nil {
		t.Fatalf("WriteSnapshot after checkpoint: %v", err)
	}

	last, err := LoadLastSnapshot(path)
	if err != nil {
		t.Fatalf("LoadLastSnapshot: %v", err)
	}
	if last.NextFile != 4 {
		t.Fatalf("NextFile: got %d want %d", last.NextFile, 4)
	}
}
