package db

import "testing"

func TestNewVersionFromLevels_ClonesInput(t *testing.T) {
	src := []level{
		{
			id:      0,
			dir:     "/tmp/l0",
			l0Paths: []string{"000003.sst", "000002.sst"},
		},
		{
			id:  1,
			dir: "/tmp/l1",
			runs: []levelFile{
				{id: 10, path: "/tmp/l1/000010.sst", minKey: "a", maxKey: "m"},
				{id: 11, path: "/tmp/l1/000011.sst", minKey: "n", maxKey: "z"},
			},
		},
	}

	v := newVersionFromLevels(src)
	if v == nil {
		t.Fatal("newVersionFromLevels returned nil")
	}

	// 先确认内容一致
	if len(v.levels) != 2 {
		t.Fatalf("want 2 levels, got %d", len(v.levels))
	}
	if got := len(v.levels[0].l0Paths); got != 2 {
		t.Fatalf("want l0Paths len=2, got %d", got)
	}
	if got := len(v.levels[1].runs); got != 2 {
		t.Fatalf("want l1 runs len=2, got %d", got)
	}

	// 修改原始输入，Version 不应受影响
	src[0].l0Paths[0] = "MUTATED-L0"
	src[1].runs[0].path = "MUTATED-RUN"
	src[1].runs = append(src[1].runs, levelFile{
		id: 12, path: "/tmp/l1/000012.sst", minKey: "zz", maxKey: "zzz",
	})

	if got := v.levels[0].l0Paths[0]; got != "000003.sst" {
		t.Fatalf("version l0Paths mutated by source change, got %q", got)
	}
	if got := v.levels[1].runs[0].path; got != "/tmp/l1/000010.sst" {
		t.Fatalf("version runs mutated by source change, got %q", got)
	}
	if got := len(v.levels[1].runs); got != 2 {
		t.Fatalf("version runs len changed by source append, got %d", got)
	}
}

func TestVersion_WithLevels_DoesNotMutateOldVersion(t *testing.T) {
	oldv := newVersionFromLevels([]level{
		{
			id:      0,
			dir:     "/tmp/l0",
			l0Paths: []string{"000002.sst", "000001.sst"},
		},
		{
			id:  1,
			dir: "/tmp/l1",
			runs: []levelFile{
				{id: 10, path: "/tmp/l1/000010.sst", minKey: "a", maxKey: "m"},
			},
		},
	})

	newv := oldv.withLevels()
	if newv == nil {
		t.Fatal("withLevels returned nil")
	}

	// 修改 newv，oldv 不应受影响
	newv.levels[0].l0Paths[0] = "NEWEST.sst"
	newv.levels[0].l0Paths = append([]string{"HEAD.sst"}, newv.levels[0].l0Paths...)
	newv.levels[1].runs[0].path = "/tmp/l1/MUTATED.sst"
	newv.levels[1].runs = append(newv.levels[1].runs, levelFile{
		id: 11, path: "/tmp/l1/000011.sst", minKey: "n", maxKey: "z",
	})

	// oldv 必须保持原样
	if got := oldv.levels[0].l0Paths[0]; got != "000002.sst" {
		t.Fatalf("old version l0Paths changed, got %q", got)
	}
	if got := len(oldv.levels[0].l0Paths); got != 2 {
		t.Fatalf("old version l0Paths len changed, got %d", got)
	}
	if got := oldv.levels[1].runs[0].path; got != "/tmp/l1/000010.sst" {
		t.Fatalf("old version run path changed, got %q", got)
	}
	if got := len(oldv.levels[1].runs); got != 1 {
		t.Fatalf("old version runs len changed, got %d", got)
	}

	// newv 应反映修改
	if got := newv.levels[0].l0Paths[0]; got != "HEAD.sst" {
		t.Fatalf("new version not updated as expected, got l0Paths[0]=%q", got)
	}
	if got := len(newv.levels[1].runs); got != 2 {
		t.Fatalf("new version runs len mismatch, got %d", got)
	}
}

func TestVersion_WithLevels_NilReceiver(t *testing.T) {
	var v *Version
	got := v.withLevels()
	if got == nil {
		t.Fatal("want non-nil version from nil receiver")
	}
	if len(got.levels) != 0 {
		t.Fatalf("want empty levels, got %d", len(got.levels))
	}
}
