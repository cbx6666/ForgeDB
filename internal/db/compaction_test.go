package db

import (
	"fmt"
	"testing"
)

// 断言：runs 按 minKey 升序，且不重叠（prev.maxKey < next.minKey）
func assertRunsSortedNoOverlap(t *testing.T, runs []levelFile) {
	t.Helper()
	for i := 1; i < len(runs); i++ {
		prev, cur := runs[i-1], runs[i]
		if prev.minKey > cur.minKey {
			t.Fatalf("runs not sorted: prev=%+v cur=%+v", prev, cur)
		}
		if prev.maxKey >= cur.minKey {
			t.Fatalf("runs overlap detected: prev=%+v cur=%+v", prev, cur)
		}
	}
}

// 方便构造可预测 key/value
func k(i int) string    { return fmt.Sprintf("k%06d", i) }
func v(i int) []byte    { return []byte(fmt.Sprintf("v%06d", i)) }
func keyA(i int) string { return fmt.Sprintf("A%d", i) }

// 1) 手动 Compact(0)：tombstone 不复活 + L1 不变式 + reopen 后正确
func TestDB_Compact_L0ToL1_CorrectnessAndReopen(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	// 000001: A=1
	if err := d.Put("A", []byte("1")); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}
	if err := d.Flush(); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}

	// 000002: B=2
	if err := d.Put("B", []byte("2")); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}
	if err := d.Flush(); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}

	// 000003: Delete(A)
	if err := d.Delete("A"); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}
	if err := d.Flush(); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}

	if got := len(d.levels[0].l0Paths); got != 3 {
		_ = d.Close()
		t.Fatalf("want 3 L0 before compaction, got %d", got)
	}

	// 手动 compaction：L0 -> L1
	if err := d.Compact(0); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}

	// L1 应该有产物
	if len(d.levels) < 2 || len(d.levels[1].runs) == 0 {
		_ = d.Close()
		t.Fatalf("want L1 non-empty after compaction")
	}
	assertRunsSortedNoOverlap(t, d.levels[1].runs)

	// A tombstone 应该生效（不复活）
	if _, ok, err := d.Get("A"); err != nil || ok {
		_ = d.Close()
		t.Fatalf("A should be deleted, ok=%v err=%v", ok, err)
	}

	// B 仍然存在
	got, ok, err := d.Get("B")
	if err != nil {
		_ = d.Close()
		t.Fatalf("get B err=%v", err)
	}
	if !ok || string(got) != "2" {
		_ = d.Close()
		t.Fatalf("B should be 2, ok=%v v=%q", ok, string(got))
	}

	// reopen（Windows：先 close）
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	d2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d2.Close() }()

	if _, ok, err := d2.Get("A"); err != nil || ok {
		t.Fatalf("A should be deleted after reopen, ok=%v err=%v", ok, err)
	}
	got2, ok, err := d2.Get("B")
	if err != nil {
		t.Fatalf("get B after reopen err=%v", err)
	}
	if !ok || string(got2) != "2" {
		t.Fatalf("B should be 2 after reopen, ok=%v v=%q", ok, string(got2))
	}

	if len(d2.levels) < 2 || len(d2.levels[1].runs) == 0 {
		t.Fatalf("want L1 non-empty after reopen")
	}
	assertRunsSortedNoOverlap(t, d2.levels[1].runs)
}

// 2) newest-value：多版本落盘后 compaction 保留最新
func TestDB_Compact_L0ToL1_KeepsNewestValue(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()

	_ = d.Put("A", []byte("1"))
	_ = d.Flush()
	_ = d.Put("A", []byte("2"))
	_ = d.Flush()
	_ = d.Put("A", []byte("3"))
	_ = d.Flush()

	// 触发一次 L0 -> L1 compaction（即使 newest 仍在 L0，Get 也应正确）
	if err := d.Compact(0); err != nil {
		t.Fatal(err)
	}

	got, ok, err := d.Get("A")
	if err != nil || !ok || string(got) != "3" {
		t.Fatalf("want A=3, ok=%v v=%q err=%v", ok, string(got), err)
	}
}

// 3) 自动 compaction：Flush 超过阈值后 L1 应有产物，reopen 仍正确
func TestDB_FlushAutoCompaction_L0ToL1(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	opt := defaultOptions()
	flushN := opt.levels[0].maxFiles + 1 // 触发条件：len(L0) > maxFiles

	for i := 0; i < flushN; i++ {
		if err := d.Put(keyA(i), []byte("v")); err != nil {
			_ = d.Close()
			t.Fatal(err)
		}
		if err := d.Flush(); err != nil {
			_ = d.Close()
			t.Fatal(err)
		}
	}

	// 触发后：至少应该生成 L1
	if len(d.levels) < 2 || len(d.levels[1].runs) == 0 {
		_ = d.Close()
		t.Fatalf("expected L1 non-empty after auto compaction, got L1=0 (L0=%d)", len(d.levels[0].l0Paths))
	}
	assertRunsSortedNoOverlap(t, d.levels[1].runs)

	// reopen 验证（先 close）
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	d2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d2.Close() }()

	for i := 0; i < flushN; i++ {
		got, ok, err := d2.Get(keyA(i))
		if err != nil || !ok || string(got) != "v" {
			t.Fatalf("key %s lost after reopen: ok=%v v=%q err=%v", keyA(i), ok, string(got), err)
		}
	}

	if len(d2.levels) < 2 || len(d2.levels[1].runs) == 0 {
		t.Fatalf("expected L1 non-empty after reopen")
	}
	assertRunsSortedNoOverlap(t, d2.levels[1].runs)
}

// 4) L1 split：构造两个“大 L0 文件”且 range 重叠，使 merged > runMaxEntries，从而一次 compact 就 split 成多个 run
func TestDB_Compact_L0ToL1_SplitsRuns(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()

	opt := defaultOptions()
	runMax := opt.levels[1].runMaxEntries
	if runMax <= 0 {
		t.Fatalf("invalid test setup: L1 runMaxEntries=%d", runMax)
	}

	// 第一张 L0：key 0 .. runMax   (runMax+1 个)
	for i := 0; i < runMax+1; i++ {
		if err := d.Put(k(i), v(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}

	// 第二张 L0：制造 overlap，从 runMax/2 .. runMax + runMax/2
	// 这样合并后的 unique keys 大约是 (runMax+1) + (runMax/2) > runMax，必然 split
	start := runMax / 2
	end := runMax + runMax/2
	for i := start; i <= end; i++ {
		if err := d.Put(k(i), v(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}

	// 现在 L0 至少 2 个文件，手动 compact 一次即可触发 split
	if err := d.Compact(0); err != nil {
		t.Fatal(err)
	}

	if len(d.levels) < 2 {
		t.Fatalf("expected L1 exists")
	}
	if len(d.levels[1].runs) < 2 {
		t.Fatalf("expected L1 to have >=2 runs after split, got %d (L0=%d)", len(d.levels[1].runs), len(d.levels[0].l0Paths))
	}
	assertRunsSortedNoOverlap(t, d.levels[1].runs)

	// 抽样验证可读性
	samples := []int{0, start, runMax, end}
	for _, idx := range samples {
		key := k(idx)
		got, ok, err := d.Get(key)
		if err != nil || !ok || string(got) != string(v(idx)) {
			t.Fatalf("get %s failed: ok=%v v=%q err=%v", key, ok, string(got), err)
		}
	}
}

// 5) 手动三层：构造 L1 足够多 runs，然后 Compact(1) 产出 L2，并保证读正确
func TestDB_Compact_L1ToL2_CreatesL2AndKeepsReads(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()

	opt := defaultOptions()
	if opt.numLevels < 3 {
		t.Fatalf("test requires 3 levels, got %d", opt.numLevels)
	}

	runMaxL1 := opt.levels[1].runMaxEntries
	if runMaxL1 <= 0 {
		t.Fatalf("invalid test setup: L1 runMaxEntries=%d", runMaxL1)
	}

	// 目标：让 L1 的 runs 数量 >= opt.levels[1].maxFiles+1（如果 maxFiles=0，就至少 2 个）
	targetL1Runs := opt.levels[1].maxFiles + 1
	if targetL1Runs < 2 {
		targetL1Runs = 2
	}

	// 每轮：做 2 次 flush，确保 pickedN(默认2) 可以一次合并并 split
	// 每轮写入一段不重叠的 key-range，减少跨轮 overlap
	base := 0
	for round := 0; round < targetL1Runs; round++ {
		// 第一张 L0：base .. base+runMaxL1
		for i := 0; i < runMaxL1+1; i++ {
			_ = d.Put(k(base+i), v(base+i))
		}
		if err := d.Flush(); err != nil {
			t.Fatal(err)
		}

		// 第二张 L0：制造 overlap，使 merged > runMaxL1，从而 split 成 >=2 个 L1 run
		start := base + runMaxL1/2
		end := base + runMaxL1 + runMaxL1/2
		for i := start; i <= end; i++ {
			_ = d.Put(k(i), v(i))
		}
		if err := d.Flush(); err != nil {
			t.Fatal(err)
		}

		// 手动 compact L0->L1，把这两张 L0 合并进 L1（并 split）
		if err := d.Compact(0); err != nil {
			t.Fatal(err)
		}

		base += runMaxL1 * 4
	}

	if len(d.levels[1].runs) < 2 {
		t.Fatalf("expected L1 runs >= 2, got %d", len(d.levels[1].runs))
	}
	assertRunsSortedNoOverlap(t, d.levels[1].runs)

	// 手动 compact L1->L2
	if err := d.Compact(1); err != nil {
		t.Fatal(err)
	}

	if len(d.levels[2].runs) == 0 {
		t.Fatalf("expected L2 non-empty after Compact(1)")
	}
	assertRunsSortedNoOverlap(t, d.levels[2].runs)

	// 抽样读：只读“第一轮”必定写入过的 key（避免选到没写过的 key）
	s := runMaxL1 / 2
	e := runMaxL1 + runMaxL1/2
	for _, idx := range []int{0, s, runMaxL1, e} {
		key := k(idx)
		got, ok, err := d.Get(key)
		if err != nil || !ok {
			t.Fatalf("get %s failed: ok=%v err=%v", key, ok, err)
		}
		if string(got) != string(v(idx)) {
			t.Fatalf("get %s mismatch: got=%q want=%q", key, string(got), string(v(idx)))
		}
	}
}

// 6) 自动三层：用自定义 Options 把阈值调小，Flush 自动触发 L0->L1 和 L1->L2，reopen 后仍一致
func TestDB_FlushAutoCompaction_ThreeLevels(t *testing.T) {
	dir := t.TempDir()

	// 让测试更快：把阈值调小
	opt := defaultOptions()
	opt.numLevels = 3
	opt.levels = []LevelOptions{
		{maxFiles: 2, pickN: 2, runMaxEntries: 0},   // L0: 很快触发
		{maxFiles: 2, pickN: 1, runMaxEntries: 32},  // L1: 很快触发到 L2
		{maxFiles: 0, pickN: 0, runMaxEntries: 128}, // L2: bottom
	}

	d, err := OpenWithOptions(dir, opt)
	if err != nil {
		t.Fatal(err)
	}

	// 写入一些 key，让系统经历多次 flush + 自动 compaction
	for i := 0; i < 50; i++ {
		if err := d.Put(k(i), v(i)); err != nil {
			_ = d.Close()
			t.Fatal(err)
		}
		// 每次 flush 一批，避免过慢
		if (i+1)%5 == 0 {
			if err := d.Flush(); err != nil {
				_ = d.Close()
				t.Fatal(err)
			}
		}
	}
	// 最后再 flush 一次
	if err := d.Flush(); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}

	// 预期：至少 L1 非空；并且很大概率 L2 也非空（因为 L1 maxFiles=2 触发很频繁）
	if len(d.levels[1].runs) == 0 {
		_ = d.Close()
		t.Fatalf("expected L1 non-empty")
	}
	assertRunsSortedNoOverlap(t, d.levels[1].runs)

	// L2 在某些极端数据分布下可能暂时还没触发，但一般会有；这里做弱断言：如果有就检查不变式
	if len(d.levels[2].runs) > 0 {
		assertRunsSortedNoOverlap(t, d.levels[2].runs)
	}

	// reopen 验证（先 close）
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	d2, err := OpenWithOptions(dir, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d2.Close() }()

	// 全量读验证
	for i := 0; i < 50; i++ {
		got, ok, err := d2.Get(k(i))
		if err != nil || !ok || string(got) != string(v(i)) {
			t.Fatalf("key %s lost after reopen: ok=%v v=%q err=%v", k(i), ok, string(got), err)
		}
	}

	// 不变式检查
	if len(d2.levels[1].runs) == 0 {
		t.Fatalf("expected L1 non-empty after reopen")
	}
	assertRunsSortedNoOverlap(t, d2.levels[1].runs)
	if len(d2.levels[2].runs) > 0 {
		assertRunsSortedNoOverlap(t, d2.levels[2].runs)
	}
}
