package db

import (
	"fmt"
	"testing"
)

// 断言：L1 按 minKey 升序，且不重叠（prev.maxKey < next.minKey）
func assertL1SortedNoOverlap(t *testing.T, l1 []levelFile) {
	t.Helper()
	for i := 1; i < len(l1); i++ {
		prev, cur := l1[i-1], l1[i]
		if prev.minKey > cur.minKey {
			t.Fatalf("L1 not sorted: prev=%+v cur=%+v", prev, cur)
		}
		if prev.maxKey >= cur.minKey {
			t.Fatalf("L1 overlap detected: prev=%+v cur=%+v", prev, cur)
		}
	}
}

// 方便构造可预测 key
func k(i int) string { return fmt.Sprintf("k%06d", i) }
func v(i int) []byte { return []byte(fmt.Sprintf("v%06d", i)) }

// 1) 手动 Compact：tombstone 不复活 + L1 不变式 + reopen 后正确
func TestDB_CompactL0ToL1_CorrectnessAndReopen(t *testing.T) {
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

	if len(d.l0) != 3 {
		_ = d.Close()
		t.Fatalf("want 3 L0 before compaction, got %d", len(d.l0))
	}

	// 手动 compaction（本实现：只 pick 最老 2 个 L0）
	if err := d.CompactL0ToL1(); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}

	// L1 应该有产物
	if len(d.l1) == 0 {
		_ = d.Close()
		t.Fatalf("want L1 non-empty after compaction")
	}
	assertL1SortedNoOverlap(t, d.l1)

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

	if len(d2.l1) == 0 {
		t.Fatalf("want L1 non-empty after reopen")
	}
	assertL1SortedNoOverlap(t, d2.l1)
}

// 2) newest-value：多版本落盘后 compaction 保留最新
func TestDB_CompactL0ToL1_KeepsNewestValue(t *testing.T) {
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

	if err := d.CompactL0ToL1(); err != nil {
		t.Fatal(err)
	}

	got, ok, err := d.Get("A")
	if err != nil || !ok || string(got) != "3" {
		t.Fatalf("want A=3, ok=%v v=%q err=%v", ok, string(got), err)
	}
}

// 3) 自动 compaction
func TestDB_FlushAutoCompaction_L0ToL1(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	// 触发条件：len(l0) > 4
	flushN := autoCompactThreshold + 1
	for i := 0; i < flushN; i++ {
		if err := d.Put(fmt.Sprintf("A%d", i), []byte("v")); err != nil {
			_ = d.Close()
			t.Fatal(err)
		}
		if err := d.Flush(); err != nil {
			_ = d.Close()
			t.Fatal(err)
		}
	}

	// 触发后：至少应该生成 L1（L0 可能还有剩余，因为每次只 pick 2 个最老 L0）
	if len(d.l1) == 0 {
		_ = d.Close()
		t.Fatalf("expected L1 non-empty after auto compaction, got L1=0 (L0=%d)", len(d.l0))
	}
	assertL1SortedNoOverlap(t, d.l1)

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
		got, ok, err := d2.Get(fmt.Sprintf("A%d", i))
		if err != nil || !ok || string(got) != "v" {
			t.Fatalf("key A%d lost after reopen: ok=%v v=%q err=%v", i, ok, string(got), err)
		}
	}

	if len(d2.l1) == 0 {
		t.Fatalf("expected L1 non-empty after reopen")
	}
	assertL1SortedNoOverlap(t, d2.l1)
}

// 4) L1 split：构造足够多的 unique keys，使 merged > l1RunMaxEntries
func TestDB_CompactL0ToL1_SplitsRuns(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()

	// 目标：产生 >256 个唯一 key 并落盘到多个 L0 文件里
	// 简单做法：每次 Put 一个不同 key 就 Flush，一共写 2*l1RunMaxEntries+10
	n := l1RunMaxEntries*2 + 10
	for i := 0; i < n; i++ {
		if err := d.Put(k(i), v(i)); err != nil {
			t.Fatal(err)
		}
		if err := d.Flush(); err != nil {
			t.Fatal(err)
		}
	}

	// 因为每次只 pick 2 个 L0，所以需要多次 compaction 才能把足够多 key 合进 L1，触发 split
	// 我们循环 compaction 直到 L0 变少 / 或 L1 run 数达到 2（split 成功）
	for step := 0; step < 200 && len(d.l1) < 2; step++ {
		// L0 为空就停止
		if len(d.l0) == 0 {
			break
		}
		if err := d.CompactL0ToL1(); err != nil {
			t.Fatal(err)
		}
	}

	if len(d.l1) < 2 {
		t.Fatalf("expected L1 to have >=2 runs after split, got %d (L0=%d)", len(d.l1), len(d.l0))
	}
	assertL1SortedNoOverlap(t, d.l1)

	// 抽样验证可读性
	for _, idx := range []int{0, 7, l1RunMaxEntries - 1, l1RunMaxEntries, n - 1} {
		key := k(idx)
		got, ok, err := d.Get(key)
		if err != nil || !ok || string(got) != string(v(idx)) {
			t.Fatalf("get %s failed: ok=%v v=%q err=%v", key, ok, string(got), err)
		}
	}
}
