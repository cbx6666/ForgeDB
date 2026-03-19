package db

import (
	"os"
	"path/filepath"
	"testing"
)

// ======== helpers (white-box safe) ========

// read internal state under lock to avoid -race
type flushState struct {
	immNil  bool
	memNil  bool
	l0Count int
	immWAL  string
	wal     string
}

func snapshotFlushStateLocked(d *DB) flushState {
	d.mu.Lock()
	defer d.mu.Unlock()

	levels := d.currentVersion().levels

	s := flushState{
		immNil: d.imm == nil,
		memNil: d.mem == nil,
		wal:    d.walPath,
		immWAL: d.immWalPath,
	}
	if len(levels) > 0 {
		s.l0Count = len(levels[0].l0Paths)
	}
	return s
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// ======== tests ========

// 1) white-box: prepareFlushLocked 必须完成 mem -> imm 冻结与 WAL 轮换
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

	// prepare（白盒调用）
	d.mu.Lock()
	job, err := d.prepareFlushLocked()
	if err != nil {
		d.mu.Unlock()
		t.Fatal(err)
	}
	if job == nil {
		d.mu.Unlock()
		t.Fatal("prepareFlushLocked returned nil job")
	}

	levels := d.currentVersion().levels

	// 1) imm 必须非空
	if d.imm == nil {
		d.mu.Unlock()
		t.Fatal("expected imm != nil after prepareFlushLocked")
	}
	// 2) old 必须在 imm 中
	if e, ok := d.imm.GetAll("old"); !ok || e.Tombstone || string(e.Value) != "v-old" {
		d.mu.Unlock()
		t.Fatalf("expected old in imm with value v-old, ok=%v tomb=%v", ok, ok && e.Tombstone)
	}
	// 3) old 不应该还在 active mem（因为已切换到新 mem）
	if _, ok := d.mem.GetAll("old"); ok {
		d.mu.Unlock()
		t.Fatal("expected old NOT in active mem after prepareFlushLocked")
	}
	// 4) prepare 后 L0 还没安装新表
	if len(levels) == 0 || len(levels[0].l0Paths) != 0 {
		got := 0
		if len(levels) > 0 {
			got = len(levels[0].l0Paths)
		}
		d.mu.Unlock()
		t.Fatalf("expected L0 unchanged before install, got L0=%d", got)
	}
	immWal := d.immWalPath
	activeWal := d.walPath
	d.mu.Unlock()

	// 5) WAL 文件应已经轮换：forge.flush.wal 应存在
	if !fileExists(immWal) {
		t.Fatalf("expected flush wal exists after prepare: %s", immWal)
	}
	// 6) active wal 也应该存在（新打开）
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
	err = d.installFlushLocked(job)
	d.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
}

// 2) white-box + black-box: flush 期间前台继续写，新旧都能读；install 后 imm 清空、L0 增加、flush wal 删除
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

	// prepare
	d.mu.Lock()
	job, err := d.prepareFlushLocked()
	if err != nil {
		d.mu.Unlock()
		t.Fatal(err)
	}
	if job == nil {
		d.mu.Unlock()
		t.Fatal("prepareFlushLocked returned nil job")
	}
	flushWal := d.immWalPath
	d.mu.Unlock()

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
	err = d.installFlushLocked(job)
	immNil := (d.imm == nil)
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

// 3) white-box crash simulation: prepare 后不 do/install，重启必须从 forge.flush.wal + forge.wal 恢复
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

	// prepare 成功，但不 do/install（模拟崩溃前中间态）
	d.mu.Lock()
	job, err := d.prepareFlushLocked()
	if err != nil {
		d.mu.Unlock()
		_ = d.Close()
		t.Fatal(err)
	}
	if job == nil {
		d.mu.Unlock()
		_ = d.Close()
		t.Fatal("prepareFlushLocked returned nil job")
	}
	flushWal := d.immWalPath
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

	// prepare 后写新数据（进入新 active wal）
	if err := d.Put("new1", []byte("vn1")); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}

	// 模拟崩溃：直接关闭（此时 flush wal 仍保留）
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

	d.mu.Lock()
	job, err := d.prepareFlushLocked()
	d.mu.Unlock()
	if err != nil {
		_ = d.Close()
		t.Fatal(err)
	}
	if job == nil {
		_ = d.Close()
		t.Fatal("prepareFlushLocked returned nil job")
	}

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
		if fileExists(s.immWAL) {
			t.Fatalf("expected flush wal removed after flush #%d: %s", i, s.immWAL)
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

// 5) extra white-box: installFlushLocked 前后文件路径应符合约定（tmp 不残留，final 在 L0）
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
	d.mu.Lock()
	job, err := d.prepareFlushLocked()
	d.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if job == nil {
		t.Fatal("prepareFlushLocked returned nil job")
	}

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
	err = d.installFlushLocked(job)
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
