package db

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// db_flush_test.go 验证显式 flush 的状态转换、落盘安装和恢复语义。

// ======== 测试辅助 ========

// 在持锁状态下读取内部状态，避免与并发测试路径产生竞态。
type flushSnapshot struct {
	immNil  bool
	memNil  bool
	l0Count int
	headWAL string
	wal     string
}

// snapshotFlushStateLocked 抓取 flush 相关的内部状态快照。
func snapshotFlushStateLocked(d *DB) flushSnapshot {
	d.mu.Lock()
	defer d.mu.Unlock()

	levels := d.currentVersion().levels

	s := flushSnapshot{
		immNil: len(d.imms) == 0,
		memNil: d.mem == nil,
		wal:    d.walPath,
	}
	if len(d.imms) > 0 && d.imms[0] != nil {
		s.headWAL = d.imms[0].walPath
	}
	if len(levels) > 0 {
		s.l0Count = len(levels[0].l0Paths)
	}
	return s
}

// fileExists 判断指定路径当前是否存在。
func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// prepareFlushJobForTest 在锁内把当前 active memtable 冻结入队，并启动一份可执行的 flush job。
func prepareFlushJobForTest(t *testing.T, d *DB) *flushJob {
	t.Helper()

	d.mu.Lock()
	defer d.mu.Unlock()

	imm, err := d.enqueueActiveMemtableLocked()
	if err != nil {
		t.Fatal(err)
	}
	if imm == nil {
		t.Fatal("enqueueActiveMemtableLocked returned nil immutable")
	}
	job := d.startFlushJobLocked()
	if job == nil {
		t.Fatal("startFlushJobLocked returned nil job")
	}
	return job
}

// ======== tests ========

// 1) white-box: 冻结 active memtable 并启动队头 flush job 后，必须完成 immutable 入队与 WAL 轮换。
// TestDB_FlushPrepare_RotatesMemAndWAL 验证冻结入队后会生成独立 flush job，并完成 WAL 轮换。
func TestDB_FlushPrepare_RotatesMemAndWAL(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()

	// 写一条旧数据
	if err := d.Put("old", []byte("v-old")); err != nil {
		t.Fatal(err)
	}

	// 白盒冻结当前 active memtable，并为队头启动一份可执行的 flush job。
	job := prepareFlushJobForTest(t, d)

	d.mu.Lock()
	levels := d.currentVersion().levels

	// 1) imm 必须非空
	if len(d.imms) != 1 || d.imms[0] == nil || d.imms[0].mem == nil {
		d.mu.Unlock()
		t.Fatal("expected one immutable memtable after freezing active memtable")
	}
	// 2) old 必须在 imm 中
	if e, ok := d.imms[0].mem.GetAll("old"); !ok || e.Tombstone || string(e.Value) != "v-old" {
		d.mu.Unlock()
		t.Fatalf("expected old in imm with value v-old, ok=%v tomb=%v", ok, ok && e.Tombstone)
	}
	// 3) old 不应该还在 active mem（因为已切换到新 mem）
	if _, ok := d.mem.GetAll("old"); ok {
		d.mu.Unlock()
		t.Fatal("expected old NOT in active mem after freezing active memtable")
	}
	// 4) immutable 入队并生成 flush job 后，L0 还没安装新表。
	if len(levels) == 0 || len(levels[0].l0Paths) != 0 {
		got := 0
		if len(levels) > 0 {
			got = len(levels[0].l0Paths)
		}
		d.mu.Unlock()
		t.Fatalf("expected L0 unchanged before install, got L0=%d", got)
	}
	immWal := d.imms[0].walPath
	activeWal := d.walPath
	d.mu.Unlock()

	// 5) WAL 文件应已经轮换：本轮独立的 flush WAL 应存在。
	if !fileExists(immWal) {
		t.Fatalf("expected flush wal exists after prepare: %s", immWal)
	}
	// 6) active wal 也应该存在（已为后续写入重新打开）。
	if !fileExists(activeWal) {
		t.Fatalf("expected active wal exists after prepare: %s", activeWal)
	}

	// 读语义（黑盒）：old 仍然可读（来自 imm）
	got, ok, err := d.Get("old")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(got) != "v-old" {
		t.Fatalf("want old=v-old after prepare, ok=%v got=%q", ok, string(got))
	}

	// do/install 收尾，避免影响其他逻辑
	if err := d.doFlush(job); err != nil {
		t.Fatal(err)
	}
	d.mu.Lock()
	err = d.installFlushLocked()
	d.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
}

// 2) white-box + black-box: flush 期间前台继续写，新旧都能读；install 后 imm 清空、L0 增加、flush wal 删除
// TestDB_Flush_ForegroundWritesDuringFlushAndInstallCleansUp 验证 flush 期间前台写入与安装清理语义。
func TestDB_Flush_ForegroundWritesDuringFlushAndInstallCleansUp(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()

	// 写旧数据
	if err := d.Put("old", []byte("v-old")); err != nil {
		t.Fatal(err)
	}

	// 白盒冻结当前 active memtable，并启动一份可执行的 flush job。
	job := prepareFlushJobForTest(t, d)
	flushWal := job.imm.walPath

	// flush 中继续写新数据
	if err := d.Put("new", []byte("v-new")); err != nil {
		t.Fatal(err)
	}

	// 黑盒读：新旧都应可读
	gotOld, okOld, err := d.Get("old")
	if err != nil {
		t.Fatal(err)
	}
	if !okOld || string(gotOld) != "v-old" {
		t.Fatalf("want old=v-old during flush, ok=%v got=%q", okOld, string(gotOld))
	}

	gotNew, okNew, err := d.Get("new")
	if err != nil {
		t.Fatal(err)
	}
	if !okNew || string(gotNew) != "v-new" {
		t.Fatalf("want new=v-new during flush, ok=%v got=%q", okNew, string(gotNew))
	}

	// do flush（锁外）
	if err := d.doFlush(job); err != nil {
		t.Fatal(err)
	}

	// install（锁内）
	d.mu.Lock()
	levelsBefore := d.currentVersion().levels
	l0Before := 0
	if len(levelsBefore) > 0 {
		l0Before = len(levelsBefore[0].l0Paths)
	}
	err = d.installFlushLocked()
	immNil := (len(d.imms) == 0)
	levelsAfter := d.currentVersion().levels
	l0After := 0
	if len(levelsAfter) > 0 {
		l0After = len(levelsAfter[0].l0Paths)
	}
	d.mu.Unlock()

	if err != nil {
		t.Fatal(err)
	}
	if !immNil {
		t.Fatalf("expected imm cleared after install")
	}
	if l0After != l0Before+1 {
		t.Fatalf("expected L0 count +1 after install, before=%d after=%d", l0Before, l0After)
	}

	// install 后 flush wal 会被删除
	if fileExists(flushWal) {
		t.Fatalf("expected flush wal removed after install: %s", flushWal)
	}

	// 终态读：新旧仍可读
	gotOld2, okOld2, err := d.Get("old")
	if err != nil {
		t.Fatal(err)
	}
	if !okOld2 || string(gotOld2) != "v-old" {
		t.Fatalf("want old=v-old after install, ok=%v got=%q", okOld2, string(gotOld2))
	}
	gotNew2, okNew2, err := d.Get("new")
	if err != nil {
		t.Fatal(err)
	}
	if !okNew2 || string(gotNew2) != "v-new" {
		t.Fatalf("want new=v-new after install, ok=%v got=%q", okNew2, string(gotNew2))
	}
}

// 3) white-box crash simulation: immutable 已入队但尚未 do/install，重启必须从 flush WAL 队列与 forge.wal 恢复。
// TestDB_Reopen_RecoversFlushWALAndActiveWAL 验证中间态下重启能从两类 WAL 恢复。
func TestDB_Reopen_RecoversFlushWALAndActiveWAL(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	// 写两条旧数据（将进入 flush wal）
	if err := d.Put("old1", []byte("v1")); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}
	if err := d.Put("old2", []byte("v2")); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}

	// immutable 已入队并生成 flush job，但不 do/install（模拟崩溃前中间态）。
	job := prepareFlushJobForTest(t, d)
	d.mu.Lock()
	flushWal := job.imm.walPath
	activeWal := d.walPath
	d.mu.Unlock()

	// 白盒确认：flush wal 存在
	if !fileExists(flushWal) {
		_ = d.Close()
		t.Fatalf("expected flush wal exists before crash: %s", flushWal)
	}
	// active wal 也应存在
	if !fileExists(activeWal) {
		_ = d.Close()
		t.Fatalf("expected active wal exists before crash: %s", activeWal)
	}

	// 入队后继续写新数据（进入新 active wal）。
	if err := d.Put("new1", []byte("vn1")); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}

	// 模拟崩溃：直接关闭（此时队头 flush wal 仍保留）。
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	// 重启
	d2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d2.Close() }()

	// old1/old2 应从 flush wal 恢复
	got1, ok1, err := d2.Get("old1")
	if err != nil {
		t.Fatal(err)
	}
	if !ok1 || string(got1) != "v1" {
		t.Fatalf("want old1=v1 after reopen, ok=%v got=%q", ok1, string(got1))
	}

	got2, ok2, err := d2.Get("old2")
	if err != nil {
		t.Fatal(err)
	}
	if !ok2 || string(got2) != "v2" {
		t.Fatalf("want old2=v2 after reopen, ok=%v got=%q", ok2, string(got2))
	}

	// new1 应从 active wal 恢复
	got3, ok3, err := d2.Get("new1")
	if err != nil {
		t.Fatal(err)
	}
	if !ok3 || string(got3) != "vn1" {
		t.Fatalf("want new1=vn1 after reopen, ok=%v got=%q", ok3, string(got3))
	}
}

// 3.1) white-box crash simulation: SST 已经写出，但 install 前崩溃；重启后仍应以两份 WAL 为准恢复。
// TestDB_Reopen_RecoversWhenFlushSSTExistsButInstallNotDone 验证未安装 SST 的重启恢复语义。
func TestDB_Reopen_RecoversWhenFlushSSTExistsButInstallNotDone(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	if err := d.Put("old", []byte("v-old")); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}

	job := prepareFlushJobForTest(t, d)

	if err := d.Put("new", []byte("v-new")); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}

	if err := d.doFlush(job); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}
	if !fileExists(job.path) {
		_ = d.Close()
		t.Fatalf("expected flushed sst exists before simulated crash: %s", job.path)
	}

	// 模拟崩溃：SST 已经落盘，但 manifest/version 尚未安装。
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	d2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d2.Close() }()

	gotOld, okOld, err := d2.Get("old")
	if err != nil {
		t.Fatal(err)
	}
	if !okOld || string(gotOld) != "v-old" {
		t.Fatalf("want old=v-old after reopen, ok=%v got=%q", okOld, string(gotOld))
	}

	gotNew, okNew, err := d2.Get("new")
	if err != nil {
		t.Fatal(err)
	}
	if !okNew || string(gotNew) != "v-new" {
		t.Fatalf("want new=v-new after reopen, ok=%v got=%q", okNew, string(gotNew))
	}
	if fileExists(job.path) {
		t.Fatalf("expected orphan sst cleaned on reopen: %s", job.path)
	}
}

// 4) sanity: 多次 Flush 后 imm 不残留、L0 递增、读语义正确
// TestDB_MultipleFlushes_NoImmLeakAndReadsCorrect 验证多次 flush 后不会残留 imm 且读取正确。
func TestDB_MultipleFlushes_NoImmLeakAndReadsCorrect(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()

	// 每次写入不同 key，然后 flush
	keys := []string{"k1", "k2", "k3"}
	vals := []string{"v1", "v2", "v3"}

	for i := 0; i < len(keys); i++ {
		if err := d.Put(keys[i], []byte(vals[i])); err != nil {
			t.Fatal(err)
		}
		if err := d.Flush(); err != nil {
			t.Fatal(err)
		}

		s := snapshotFlushStateLocked(d)
		if !s.immNil {
			t.Fatalf("expected imm=nil after flush #%d", i)
		}
		if s.l0Count != i+1 {
			t.Fatalf("expected L0 count=%d after flush #%d, got %d", i+1, i, s.l0Count)
		}
		// flush wal 应该不存在（install 会删）
		if s.headWAL != "" && fileExists(s.headWAL) {
			t.Fatalf("expected head flush wal removed after flush #%d: %s", i, s.headWAL)
		}
	}

	// 所有 key 读正确
	for i := 0; i < len(keys); i++ {
		got, ok, err := d.Get(keys[i])
		if err != nil {
			t.Fatal(err)
		}
		if !ok || string(got) != vals[i] {
			t.Fatalf("want %s=%q, ok=%v got=%q", keys[i], vals[i], ok, string(got))
		}
	}
}

// 4.1) white-box: 第一份 immutable memtable 尚未 install 时，第二份也应允许入队，并在 install 后成为新的队头。
// TestDB_FlushQueue_SecondImmutableBecomesNewHead 验证单 worker + immutable queue 的基本排队语义。
func TestDB_FlushQueue_SecondImmutableBecomesNewHead(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()

	if err := d.Put("k1", []byte("v1")); err != nil {
		t.Fatal(err)
	}

	// 先准备第一份 flush job，但暂不 install，模拟“队头仍在途”的状态。
	job1 := prepareFlushJobForTest(t, d)

	if err := d.Put("k2", []byte("v2")); err != nil {
		t.Fatal(err)
	}

	// 第一份 immutable 尚未 install 时，再把新的 active mem 冻结入队。
	d.mu.Lock()
	imm2, err := d.enqueueActiveMemtableLocked()
	if err != nil {
		d.mu.Unlock()
		t.Fatal(err)
	}
	if imm2 == nil {
		d.mu.Unlock()
		t.Fatal("enqueueActiveMemtableLocked returned nil immutable")
	}
	if got := len(d.imms); got != 2 {
		d.mu.Unlock()
		t.Fatalf("expected 2 immutable memtables queued, got %d", got)
	}
	if d.imms[0].walPath != job1.imm.walPath {
		d.mu.Unlock()
		t.Fatal("expected queue head still points to the first immutable wal")
	}
	d.mu.Unlock()

	if err := d.doFlush(job1); err != nil {
		t.Fatal(err)
	}

	// 安装第一份 SST 后，第二份 immutable 应自动成为新的队头。
	d.mu.Lock()
	err = d.installFlushLocked()
	if err != nil {
		d.mu.Unlock()
		t.Fatal(err)
	}
	if got := len(d.imms); got != 1 {
		d.mu.Unlock()
		t.Fatalf("expected 1 immutable memtable remaining after first install, got %d", got)
	}
	if d.imms[0] != imm2 {
		d.mu.Unlock()
		t.Fatal("expected second immutable memtable promoted to queue head")
	}
	if d.imms[0].walPath != imm2.walPath {
		d.mu.Unlock()
		t.Fatal("expected queue head updated to the second immutable wal")
	}
	d.mu.Unlock()

	got1, ok1, err := d.Get("k1")
	if err != nil {
		t.Fatal(err)
	}
	if !ok1 || string(got1) != "v1" {
		t.Fatalf("want k1=v1 after first install, ok=%v got=%q", ok1, string(got1))
	}

	got2, ok2, err := d.Get("k2")
	if err != nil {
		t.Fatal(err)
	}
	if !ok2 || string(got2) != "v2" {
		t.Fatalf("want k2=v2 while second immutable is queued, ok=%v got=%q", ok2, string(got2))
	}

	// 最后走公开 Flush，把第二份队列数据也完全落盘，避免残留影响后续状态。
	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}
}

// 5) extra white-box: installFlushLocked 前后文件路径应符合约定（tmp 不残留，final 在 L0）
// TestDB_Flush_FileLayout_NoTmpLeft 验证 flush 后不会残留临时文件。
func TestDB_Flush_FileLayout_NoTmpLeft(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()

	if err := d.Put("A", []byte("1")); err != nil {
		t.Fatal(err)
	}

	// prepare
	job := prepareFlushJobForTest(t, d)

	// do
	if err := d.doFlush(job); err != nil {
		t.Fatal(err)
	}

	// tmp 不应存在（rename 后）
	if fileExists(job.tmpPath) {
		t.Fatalf("expected tmp removed after rename: %s", job.tmpPath)
	}
	// final 应存在
	if !fileExists(job.path) {
		t.Fatalf("expected final sst exists after doFlush: %s", job.path)
	}

	// install
	d.mu.Lock()
	err = d.installFlushLocked()
	d.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}

	// L0 应包含该文件路径（newest-first，放在头部）
	d.mu.Lock()
	levels := d.currentVersion().levels
	if len(levels) == 0 || len(levels[0].l0Paths) == 0 || levels[0].l0Paths[0] != job.path {
		got := ""
		if len(levels) > 0 && len(levels[0].l0Paths) > 0 {
			got = levels[0].l0Paths[0]
		}
		d.mu.Unlock()
		t.Fatalf("expected L0[0]==job.path, got %q want %q", got, job.path)
	}
	d.mu.Unlock()

	// 路径应位于 sst/l0 目录下（防回归）
	wantPrefix := filepath.Join(d.sstDir, "l0") + string(os.PathSeparator)
	if len(job.path) < len(wantPrefix) || job.path[:len(wantPrefix)] != wantPrefix {
		t.Fatalf("expected sst under l0 dir, got %s want prefix %s", job.path, wantPrefix)
	}
}

// 6) white-box: 如果已经完成 prepare，显式 Flush 仍应唤醒后台 flushLoop 并等待该轮完成。
// TestDB_Flush_AdoptsPreparedJob 验证公开 Flush 会接管已 prepare 的 job。
func TestDB_Flush_AdoptsPreparedJob(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()

	if err := d.Put("prepared", []byte("v-prepared")); err != nil {
		t.Fatal(err)
	}

	prepareFlushJobForTest(t, d)

	done := make(chan error, 1)
	go func() {
		done <- d.Flush()
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Flush did not finish prepared job in time")
	}

	got, ok, err := d.Get("prepared")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(got) != "v-prepared" {
		t.Fatalf("want prepared=v-prepared after Flush, ok=%v got=%q", ok, string(got))
	}

	s := snapshotFlushStateLocked(d)
	if !s.immNil {
		t.Fatal("expected imm cleared after prepared flush")
	}
	if s.l0Count != 1 {
		t.Fatalf("expected one L0 table after prepared flush, got %d", s.l0Count)
	}
}
