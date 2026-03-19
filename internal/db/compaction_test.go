package db

import (
	"fmt"
	"testing"
	"time"
)

// ========== helpers ==========

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

// 异步后台：等待条件在 deadline 前成立，否则失败（用于“最终一致性”结构验证）
func waitUntil(t *testing.T, deadline time.Duration, tick time.Duration, cond func() bool, onTimeoutMsg func() string) {
	t.Helper()
	start := time.Now()
	for {
		if cond() {
			return
		}
		if time.Since(start) >= deadline {
			if onTimeoutMsg != nil {
				t.Fatalf("timeout after %s: %s", deadline, onTimeoutMsg())
			}
			t.Fatalf("timeout after %s", deadline)
		}
		time.Sleep(tick)
	}
}

// 方便构造可预测 key/value
func k(i int) string { return fmt.Sprintf("k%06d", i) }
func v(i int) []byte { return []byte(fmt.Sprintf("v%06d", i)) }
func keyA(i int) string {
	return fmt.Sprintf("A%d", i)
}

// 锁内读取某些结构信息，避免 -race
type levelSnapshot struct {
	l0 int
	l1 int
	l2 int
}

func snapshotLevels(d *DB) levelSnapshot {
	d.mu.Lock()
	defer d.mu.Unlock()

	levels := d.currentVersion().levels

	s := levelSnapshot{}
	if len(levels) > 0 {
		s.l0 = len(levels[0].l0Paths)
	}
	if len(levels) > 1 {
		s.l1 = len(levels[1].runs)
	}
	if len(levels) > 2 {
		s.l2 = len(levels[2].runs)
	}
	return s
}

func assertInvariantsLocked(t *testing.T, d *DB) {
	t.Helper()
	d.mu.Lock()
	defer d.mu.Unlock()

	levels := d.currentVersion().levels

	// L0 允许 overlap，不检查
	for level := 1; level < len(levels); level++ {
		if len(levels[level].runs) > 0 {
			assertRunsSortedNoOverlap(t, levels[level].runs)
		}
	}
}

func mustBuildManualL0Compaction(t *testing.T, d *DB) (*compactionJob, *compactionResult) {
	t.Helper()

	d.mu.Lock()
	job, err := d.pickCompactionJobLocked(0)
	d.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if job == nil {
		t.Fatal("expected manual L0 compaction job")
	}

	res, err := d.doCompaction(job)
	if err != nil {
		t.Fatal(err)
	}
	if res == nil || len(res.newRuns) == 0 {
		t.Fatal("expected compaction outputs")
	}
	for _, run := range res.newRuns {
		if !fileExists(run.path) {
			t.Fatalf("expected compaction output exists: %s", run.path)
		}
	}
	return job, res
}

func installCompactionVersionWithoutCleanup(t *testing.T, d *DB, res *compactionResult) []string {
	t.Helper()

	job := res.job

	d.mu.Lock()
	defer d.mu.Unlock()

	oldv := d.currentVersion()
	newv := oldv.withLevels()

	src := &newv.levels[job.level]
	dst := &newv.levels[job.level+1]

	if job.level == 0 {
		src.l0Paths = removePaths(src.l0Paths, job.srcPaths)
	} else {
		src.runs = removeRunsByPaths(src.runs, job.srcPaths)
	}

	overIdx := overlappedRuns(dst.runs, job.minKey, job.maxKey)
	realOldDst := make([]string, 0, len(overIdx))
	for _, i := range overIdx {
		realOldDst = append(realOldDst, dst.runs[i].path)
	}

	newDst := make([]levelFile, 0, len(dst.runs)-len(overIdx)+len(res.newRuns))
	if len(overIdx) == 0 {
		newDst = append(newDst, dst.runs...)
		newDst = append(newDst, res.newRuns...)
		assertRunsSortedNoOverlap(t, newDst)
	} else {
		start := overIdx[0]
		end := overIdx[len(overIdx)-1] + 1
		newDst = append(newDst, dst.runs[:start]...)
		newDst = append(newDst, res.newRuns...)
		newDst = append(newDst, dst.runs[end:]...)
	}
	dst.runs = newDst

	if err := d.persistManifestLevels(newv.levels); err != nil {
		t.Fatal(err)
	}
	d.current = newv

	oldPaths := make([]string, 0, len(job.srcPaths)+len(realOldDst))
	oldPaths = append(oldPaths, job.srcPaths...)
	oldPaths = append(oldPaths, realOldDst...)

	all := make([]string, 0, len(job.srcPaths)+len(job.dstPaths))
	all = append(all, job.srcPaths...)
	all = append(all, job.dstPaths...)
	d.clearPendingLocked(all)

	return oldPaths
}

// ========== tests ==========

// 1) 基础正确性：tombstone 不复活 + reopen 后仍正确（不依赖 compaction 立刻完成）
func TestDB_TombstoneAndReopen_Correctness(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	// A=1
	if err := d.Put("A", []byte("1")); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}
	if err := d.Flush(); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}

	// B=2
	if err := d.Put("B", []byte("2")); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}
	if err := d.Flush(); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}

	// Delete(A)
	if err := d.Delete("A"); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}
	if err := d.Flush(); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}

	// 可选：触发后台 compaction（异步，不等待）
	_ = d.RequestCompaction()

	// A tombstone 生效
	if _, ok, err := d.Get("A"); err != nil || ok {
		_ = d.Close()
		t.Fatalf("A should be deleted, ok=%v err=%v", ok, err)
	}

	// B 仍存在
	got, ok, err := d.Get("B")
	if err != nil {
		_ = d.Close()
		t.Fatalf("get B err=%v", err)
	}
	if !ok || string(got) != "2" {
		_ = d.Close()
		t.Fatalf("B should be 2, ok=%v v=%q", ok, string(got))
	}

	// 如果后台已经产出 runs，则检查不变式
	assertInvariantsLocked(t, d)

	// reopen
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

	assertInvariantsLocked(t, d2)
}

// 2) newest-value：多版本落盘后，必须读到最新（与 compaction 是否完成无关）
func TestDB_KeepsNewestValue(t *testing.T) {
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

	_ = d.RequestCompaction() // 异步触发，不等待

	got, ok, err := d.Get("A")
	if err != nil || !ok || string(got) != "3" {
		t.Fatalf("want A=3, ok=%v v=%q err=%v", ok, string(got), err)
	}

	assertInvariantsLocked(t, d)
}

// 3) 针对后台 compaction：触发阈值后，最终应完成 L0->L1，并满足 L1 不变式
func TestDB_BackgroundCompaction_EventuallyL0ToL1(t *testing.T) {
	dir := t.TempDir()

	// 用小阈值加速：L0 maxFiles=2，超过就该压到 L1
	opt := defaultOptions()
	opt.numLevels = 2
	opt.levels = []LevelOptions{
		{maxFiles: 2, pickN: 2, runMaxEntries: 0},  // L0
		{maxFiles: 0, pickN: 0, runMaxEntries: 64}, // L1 bottom
	}

	d, err := OpenWithOptions(dir, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()

	// 连续 flush 3 次：len(L0)=3 > maxFiles(2)，满足压缩条件
	for i := 0; i < 3; i++ {
		if err := d.Put(keyA(i), []byte("v")); err != nil {
			t.Fatal(err)
		}
		if err := d.Flush(); err != nil {
			t.Fatal(err)
		}
	}

	// 显式请求一次（即使 Flush 已 signal，这里也更明确）
	if err := d.RequestCompaction(); err != nil {
		t.Fatal(err)
	}

	// 等待：最终 L1 应该非空（后台完成 install）
	waitUntil(
		t,
		3*time.Second,
		10*time.Millisecond,
		func() bool {
			s := snapshotLevels(d)
			return s.l1 > 0
		},
		func() string {
			s := snapshotLevels(d)
			return fmt.Sprintf("expected L1>0 eventually, got L0=%d L1=%d", s.l0, s.l1)
		},
	)

	// L1 不变式：sorted + no-overlap
	assertInvariantsLocked(t, d)

	// 读验证
	for i := 0; i < 3; i++ {
		got, ok, err := d.Get(keyA(i))
		if err != nil || !ok || string(got) != "v" {
			t.Fatalf("key %s wrong after bg compaction: ok=%v v=%q err=%v", keyA(i), ok, string(got), err)
		}
	}
}

// 4) 多层后台 compaction：最终应能把数据推进到 L2（L0->L1->L2）
func TestDB_BackgroundCompaction_EventuallyThreeLevelsToL2(t *testing.T) {
	dir := t.TempDir()

	// 小阈值：让 L0 很快触发到 L1，L1 很快触发到 L2
	opt := defaultOptions()
	opt.numLevels = 3
	opt.levels = []LevelOptions{
		{maxFiles: 2, pickN: 2, runMaxEntries: 0},   // L0
		{maxFiles: 2, pickN: 1, runMaxEntries: 32},  // L1
		{maxFiles: 0, pickN: 0, runMaxEntries: 128}, // L2 bottom
	}

	d, err := OpenWithOptions(dir, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()

	// 写入更多 key，分批 flush
	total := 120
	for i := 0; i < total; i++ {
		if err := d.Put(k(i), v(i)); err != nil {
			t.Fatal(err)
		}
		// 每 5 个 flush 一次，制造多个 L0 文件
		if (i+1)%5 == 0 {
			if err := d.Flush(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}

	// 明确请求一次 compaction（异步）
	if err := d.RequestCompaction(); err != nil {
		t.Fatal(err)
	}

	// 等待：最终 L2 有产物（不要求一定很多，但应能触发）
	waitUntil(
		t,
		5*time.Second,
		20*time.Millisecond,
		func() bool {
			s := snapshotLevels(d)
			return s.l2 > 0
		},
		func() string {
			s := snapshotLevels(d)
			return fmt.Sprintf("expected L2>0 eventually, got L0=%d L1=%d L2=%d", s.l0, s.l1, s.l2)
		},
	)

	// L1/L2 不变式检查
	assertInvariantsLocked(t, d)

	// 全量读验证
	for i := 0; i < total; i++ {
		got, ok, err := d.Get(k(i))
		if err != nil || !ok || string(got) != string(v(i)) {
			t.Fatalf("key %s wrong after bg 3-level compaction: ok=%v v=%q err=%v", k(i), ok, string(got), err)
		}
	}

	// reopen 后仍一致
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	d2, err := OpenWithOptions(dir, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d2.Close() }()

	for i := 0; i < total; i++ {
		got, ok, err := d2.Get(k(i))
		if err != nil || !ok || string(got) != string(v(i)) {
			t.Fatalf("key %s wrong after reopen: ok=%v v=%q err=%v", k(i), ok, string(got), err)
		}
	}
	assertInvariantsLocked(t, d2)
}

// 5) 后台 compaction 在运行期间，前台写入/读取仍保持正确性（这里只测“正确性”，不测性能）
func TestDB_BackgroundCompaction_ForegroundOpsRemainCorrect(t *testing.T) {
	dir := t.TempDir()

	opt := defaultOptions()
	opt.numLevels = 3
	opt.levels = []LevelOptions{
		{maxFiles: 1, pickN: 1, runMaxEntries: 0},   // 让 compaction 更频繁
		{maxFiles: 1, pickN: 1, runMaxEntries: 16},  // 更频繁推进
		{maxFiles: 0, pickN: 0, runMaxEntries: 128}, // bottom
	}

	d, err := OpenWithOptions(dir, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()

	// 先制造一些 flush，触发后台有活干
	for i := 0; i < 30; i++ {
		_ = d.Put(k(i), v(i))
		if (i+1)%3 == 0 {
			_ = d.Flush()
		}
	}
	_ = d.Flush()
	_ = d.RequestCompaction()

	// 前台继续进行 put/get（同步 API，本项目仍是大锁串行，但应保持语义正确）
	for i := 30; i < 80; i++ {
		if err := d.Put(k(i), v(i)); err != nil {
			t.Fatal(err)
		}
		// 随机读几个旧 key（确定存在）
		if i%7 == 0 {
			_, ok, err := d.Get(k(i / 2))
			if err != nil || !ok {
				t.Fatalf("expected key %s readable during bg compaction: ok=%v err=%v", k(i/2), ok, err)
			}
		}
		if (i+1)%4 == 0 {
			if err := d.Flush(); err != nil {
				t.Fatal(err)
			}
		}
	}

	// 最终读验证（不要求 compaction 完成到某层，但数据必须都在）
	for i := 0; i < 80; i++ {
		got, ok, err := d.Get(k(i))
		if err != nil || !ok || string(got) != string(v(i)) {
			t.Fatalf("key %s wrong at end: ok=%v v=%q err=%v", k(i), ok, string(got), err)
		}
	}

	assertInvariantsLocked(t, d)
}

// 6) white-box crash simulation: doCompaction 已写出新 SST，但 install 前崩溃；
// 重启后应保留 manifest 里仍引用的旧文件，并清理新的 orphan compaction outputs。
func TestDB_Reopen_RemovesOrphanCompactionOutputsBeforeInstall(t *testing.T) {
	dir := t.TempDir()

	opt := defaultOptions()
	opt.numLevels = 2
	opt.levels = []LevelOptions{
		{maxFiles: 100, pickN: 2, runMaxEntries: 0},
		{maxFiles: 0, pickN: 0, runMaxEntries: 64},
	}

	d, err := OpenWithOptions(dir, opt)
	if err != nil {
		t.Fatal(err)
	}

	if err := d.Put("A", []byte("1")); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}
	if err := d.Flush(); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}
	if err := d.Put("B", []byte("2")); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}
	if err := d.Flush(); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}

	_, res := mustBuildManualL0Compaction(t, d)
	newPaths := make([]string, 0, len(res.newRuns))
	for _, run := range res.newRuns {
		newPaths = append(newPaths, run.path)
	}

	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	d2, err := OpenWithOptions(dir, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d2.Close() }()

	for _, p := range newPaths {
		if fileExists(p) {
			t.Fatalf("expected orphan compaction output removed on reopen: %s", p)
		}
	}

	gotA, ok, err := d2.Get("A")
	if err != nil || !ok || string(gotA) != "1" {
		t.Fatalf("want A=1 after reopen, ok=%v v=%q err=%v", ok, string(gotA), err)
	}
	gotB, ok, err := d2.Get("B")
	if err != nil || !ok || string(gotB) != "2" {
		t.Fatalf("want B=2 after reopen, ok=%v v=%q err=%v", ok, string(gotB), err)
	}

	s := snapshotLevels(d2)
	if s.l0 != 2 || s.l1 != 0 {
		t.Fatalf("expected old layout preserved after reopen, got L0=%d L1=%d", s.l0, s.l1)
	}
}

// 7) white-box crash simulation: manifest 已安装新 compaction 结果，但旧文件尚未删除就崩溃；
// 重启后应保留新文件，并清理已不再被 manifest 引用的旧输入文件。
func TestDB_Reopen_CleansOldCompactionInputsAfterManifestInstalled(t *testing.T) {
	dir := t.TempDir()

	opt := defaultOptions()
	opt.numLevels = 2
	opt.levels = []LevelOptions{
		{maxFiles: 100, pickN: 2, runMaxEntries: 0},
		{maxFiles: 0, pickN: 0, runMaxEntries: 64},
	}

	d, err := OpenWithOptions(dir, opt)
	if err != nil {
		t.Fatal(err)
	}

	if err := d.Put("A", []byte("1")); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}
	if err := d.Flush(); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}
	if err := d.Put("B", []byte("2")); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}
	if err := d.Flush(); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}

	_, res := mustBuildManualL0Compaction(t, d)
	oldPaths := installCompactionVersionWithoutCleanup(t, d, res)
	for _, p := range oldPaths {
		if !fileExists(p) {
			_ = d.Close()
			t.Fatalf("expected old compaction input still exists before simulated crash: %s", p)
		}
	}

	newPaths := make([]string, 0, len(res.newRuns))
	for _, run := range res.newRuns {
		newPaths = append(newPaths, run.path)
	}

	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	d2, err := OpenWithOptions(dir, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d2.Close() }()

	for _, p := range oldPaths {
		if fileExists(p) {
			t.Fatalf("expected old compaction input removed on reopen: %s", p)
		}
	}
	for _, p := range newPaths {
		if !fileExists(p) {
			t.Fatalf("expected installed compaction output kept on reopen: %s", p)
		}
	}

	gotA, ok, err := d2.Get("A")
	if err != nil || !ok || string(gotA) != "1" {
		t.Fatalf("want A=1 after reopen, ok=%v v=%q err=%v", ok, string(gotA), err)
	}
	gotB, ok, err := d2.Get("B")
	if err != nil || !ok || string(gotB) != "2" {
		t.Fatalf("want B=2 after reopen, ok=%v v=%q err=%v", ok, string(gotB), err)
	}

	s := snapshotLevels(d2)
	if s.l0 != 0 || s.l1 == 0 {
		t.Fatalf("expected compaction layout preserved after reopen, got L0=%d L1=%d", s.l0, s.l1)
	}
	assertInvariantsLocked(t, d2)
}
