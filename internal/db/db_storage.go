package db

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"monolithdb/internal/manifest"
	"monolithdb/internal/sstable"
	"monolithdb/internal/types"
)

// db_storage.go 负责版本恢复、SST 读写和文件系统辅助逻辑。

// newVersionFromLevels 基于给定层级列表创建只读版本视图。
func newVersionFromLevels(levels []level) *Version {
	return &Version{
		levels: cloneLevels(levels),
	}
}

// withLevels 基于当前版本复制一份可修改的新版本。
func (v *Version) withLevels() *Version {
	if v == nil {
		return &Version{}
	}
	return &Version{
		levels: cloneLevels(v.levels),
	}
}

// cloneLevels 深拷贝层级元数据，避免多个版本共享底层切片。
func cloneLevels(levels []level) []level {
	if levels == nil {
		return nil
	}

	out := make([]level, len(levels))
	for i := range levels {
		out[i] = level{
			id:      levels[i].id,
			dir:     levels[i].dir,
			l0Paths: append([]string(nil), levels[i].l0Paths...),
			runs:    append([]levelFile(nil), levels[i].runs...),
		}
	}
	return out
}

// recoverFromManifest 从 MANIFEST 快照恢复层级结构。
func recoverFromManifest(manPath string, sstDir string, opt Options) ([]level, uint64, error) {
	snap, err := manifest.LoadLastSnapshot(manPath)
	if err != nil {
		return nil, 0, err
	}
	if len(snap.Levels) != opt.numLevels {
		return nil, 0, fmt.Errorf("db: manifest levels mismatch: got %d want %d", len(snap.Levels), opt.numLevels)
	}

	levels := make([]level, 0, opt.numLevels)
	for i := 0; i < opt.numLevels; i++ {
		ldir := filepath.Join(sstDir, fmt.Sprintf("l%d", i))
		if err := os.MkdirAll(ldir, 0o755); err != nil {
			return nil, 0, err
		}
		levels = append(levels, level{id: i, dir: ldir})
	}

	levels[0].l0Paths = append([]string(nil), snap.Levels[0]...)

	for li := 1; li < opt.numLevels; li++ {
		paths := snap.Levels[li]
		runs := make([]levelFile, 0, len(paths))
		for _, p := range paths {
			minK, maxK, err := sstKeyRange(p)
			if err != nil {
				return nil, 0, err
			}
			id, _ := parseSSTID(p)
			runs = append(runs, levelFile{
				id:     id,
				path:   p,
				minKey: minK,
				maxKey: maxK,
			})
		}
		sort.Slice(runs, func(i, j int) bool { return runs[i].minKey < runs[j].minKey })
		levels[li].runs = runs
	}

	return levels, snap.NextFile, nil
}

// levelsToSnapshot 把内存中的层级结构转换成 MANIFEST 快照格式。
func levelsToSnapshot(levels []level) [][]string {
	out := make([][]string, len(levels))
	for i := range levels {
		if i == 0 {
			out[i] = append([]string(nil), levels[i].l0Paths...)
			continue
		}
		paths := make([]string, 0, len(levels[i].runs))
		for _, f := range levels[i].runs {
			paths = append(paths, f.path)
		}
		out[i] = paths
	}
	return out
}

// scanLevel0 扫描 L0 目录，并返回 newest-first 的 SST 路径。
func scanLevel0(sstDir string) (paths []string, maxID uint64, err error) {
	glob := filepath.Join(sstDir, "*.sst")
	list, err := filepath.Glob(glob)
	if err != nil {
		return nil, 0, err
	}

	sort.Strings(list)

	maxID = 0
	for _, p := range list {
		id, ok := parseSSTID(p)
		if ok && id > maxID {
			maxID = id
		}
	}

	for i, j := 0, len(list)-1; i < j; i, j = i+1, j-1 {
		list[i], list[j] = list[j], list[i]
	}

	return list, maxID, nil
}

// scanLevelN 扫描 L1 及以上目录，并恢复 run 元信息。
func scanLevelN(dir string) (files []levelFile, maxID uint64, err error) {
	glob := filepath.Join(dir, "*.sst")
	list, err := filepath.Glob(glob)
	if err != nil {
		return nil, 0, err
	}

	maxID = 0
	files = make([]levelFile, 0, len(list))
	for _, p := range list {
		id, ok := parseSSTID(p)
		if ok && id > maxID {
			maxID = id
		}
		minK, maxK, err := sstKeyRange(p)
		if err != nil {
			return nil, 0, err
		}
		files = append(files, levelFile{id: id, path: p, minKey: minK, maxKey: maxK})
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].minKey < files[j].minKey
	})
	return files, maxID, nil
}

// scanAllLevels 从磁盘目录扫描并恢复全部层级。
func scanAllLevels(sstDir string, opt Options) ([]level, uint64, error) {
	levels := make([]level, 0, opt.numLevels)
	var maxID uint64

	for i := 0; i < opt.numLevels; i++ {
		ldir := filepath.Join(sstDir, fmt.Sprintf("l%d", i))
		if err := os.MkdirAll(ldir, 0o755); err != nil {
			return nil, 0, err
		}

		lv := level{id: i, dir: ldir}
		if i == 0 {
			paths, mid, err := scanLevel0(ldir)
			if err != nil {
				return nil, 0, err
			}
			lv.l0Paths = paths
			if mid > maxID {
				maxID = mid
			}
		} else {
			runs, mid, err := scanLevelN(ldir)
			if err != nil {
				return nil, 0, err
			}
			lv.runs = runs
			if mid > maxID {
				maxID = mid
			}
		}

		levels = append(levels, lv)
	}

	return levels, maxID + 1, nil
}

// cleanupTmpFiles 清理 sst 目录下遗留的临时文件。
func cleanupTmpFiles(sstDir string) error {
	dirtyDirs := make(map[string]struct{})
	if err := filepath.WalkDir(sstDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasSuffix(d.Name(), ".tmp") {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return err
			}
			dirtyDirs[filepath.Dir(path)] = struct{}{}
		}
		return nil
	}); err != nil {
		return err
	}

	for dir := range dirtyDirs {
		if err := syncDirFn(dir); err != nil {
			return err
		}
	}
	return nil
}

// sanityCheckSSTDir 清理 tmp 文件并删除未被引用的孤儿 SST。
func sanityCheckSSTDir(sstDir string, levels []level) ([]string, error) {
	if err := cleanupTmpFiles(sstDir); err != nil {
		return nil, err
	}

	all, err := listAllSSTFiles(sstDir)
	if err != nil {
		return nil, err
	}
	ref := referencedSSTSet(levels)
	orphan := findOrphanSST(all, ref)
	if len(orphan) == 0 {
		return nil, nil
	}
	for _, p := range orphan {
		if err := removeFileAndSync(p); err != nil {
			return nil, err
		}
	}
	return orphan, nil
}

// syncDirFn 会在测试中被替换，用于控制目录同步行为。
var syncDirFn = syncDirIfSupported

// syncDirIfSupported 尽力持久化目录项变更。
func syncDirIfSupported(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}

	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()

	if err := f.Sync(); err != nil {
		if isDirSyncUnsupported(err) {
			return nil
		}
		return err
	}
	return nil
}

// syncPathDir 对文件所在目录做一次同步，用于在 rename 后固化目录项变更。
func syncPathDir(path string) error {
	return syncDirFn(filepath.Dir(path))
}

// removeFileAndSync 删除文件后同步其父目录。
func removeFileAndSync(path string) error {
	var err error
	for i := 0; i < 8; i++ {
		err = os.Remove(path)
		if err == nil || os.IsNotExist(err) {
			return syncPathDir(path)
		}
		if !isTransientRemoveErr(err) {
			return err
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return syncPathDir(path)
}

func isDirSyncUnsupported(err error) bool {
	if err == nil {
		return false
	}

	if runtime.GOOS == "windows" {
		return true
	}

	var errno syscall.Errno
	if errors.As(err, &errno) {
		switch errno {
		case syscall.EINVAL, syscall.EBADF:
			return true
		}
	}

	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "not supported") || strings.Contains(msg, "incorrect function")
}

func isTransientRemoveErr(err error) bool {
	if err == nil || runtime.GOOS != "windows" {
		return false
	}

	var pathErr *os.PathError
	if errors.As(err, &pathErr) {
		var errno syscall.Errno
		if errors.As(pathErr.Err, &errno) {
			return errno == syscall.Errno(32) || errno == syscall.Errno(5)
		}
	}

	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "being used by another process") || strings.Contains(msg, "access is denied")
}

func listAllSSTFiles(sstDir string) ([]string, error) {
	var out []string
	err := filepath.WalkDir(sstDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasSuffix(d.Name(), ".sst") {
			out = append(out, path)
		}
		return nil
	})
	return out, err
}

func referencedSSTSet(levels []level) map[string]struct{} {
	set := make(map[string]struct{}, 1024)
	for li := range levels {
		lv := &levels[li]
		if li == 0 {
			for _, p := range lv.l0Paths {
				set[p] = struct{}{}
			}
		} else {
			for _, f := range lv.runs {
				set[f.path] = struct{}{}
			}
		}
	}
	return set
}

func findOrphanSST(all []string, ref map[string]struct{}) []string {
	var orphan []string
	for _, p := range all {
		if _, ok := ref[p]; !ok {
			orphan = append(orphan, p)
		}
	}
	return orphan
}

func removePaths(all []string, del []string) []string {
	if len(del) == 0 {
		return all
	}
	set := make(map[string]struct{}, len(del))
	for _, p := range del {
		set[p] = struct{}{}
	}
	out := make([]string, 0, len(all))
	for _, p := range all {
		if _, ok := set[p]; !ok {
			out = append(out, p)
		}
	}
	return out
}

func removeRunsByPaths(all []levelFile, del []string) []levelFile {
	if len(del) == 0 {
		return all
	}
	set := make(map[string]struct{}, len(del))
	for _, p := range del {
		set[p] = struct{}{}
	}
	out := make([]levelFile, 0, len(all))
	for _, f := range all {
		if _, ok := set[f.path]; !ok {
			out = append(out, f)
		}
	}
	return out
}

func parseSSTID(path string) (uint64, bool) {
	base := filepath.Base(path)
	name := strings.TrimSuffix(base, ".sst")
	id, err := strconv.ParseUint(name, 10, 64)
	if err != nil {
		return 0, false
	}
	return id, true
}

func sstKeyRange(path string) (min, max string, err error) {
	return sstable.LoadBounds(path)
}

func rangeOfFiles(paths []string) (minK, maxK string, err error) {
	first := true
	for _, p := range paths {
		a, b, err := sstKeyRange(p)
		if err != nil {
			return "", "", err
		}
		if first {
			minK, maxK = a, b
			first = false
			continue
		}
		if a < minK {
			minK = a
		}
		if b > maxK {
			maxK = b
		}
	}
	return minK, maxK, nil
}

func pickOldestL0(l0 []string, n int, skip func(string) bool) (picked, remain []string) {
	remain = make([]string, 0, len(l0))
	picked = make([]string, 0, n)

	remain = append(remain, l0...)

	for i := len(l0) - 1; i >= 0 && len(picked) < n; i-- {
		p := l0[i]
		if skip != nil && skip(p) {
			continue
		}
		picked = append(picked, p)
	}

	for i, j := 0, len(picked)-1; i < j; i, j = i+1, j-1 {
		picked[i], picked[j] = picked[j], picked[i]
	}

	if len(picked) == 0 {
		return nil, remain
	}
	set := make(map[string]struct{}, len(picked))
	for _, p := range picked {
		set[p] = struct{}{}
	}
	out := make([]string, 0, len(remain)-len(picked))
	for _, p := range remain {
		if _, ok := set[p]; !ok {
			out = append(out, p)
		}
	}
	remain = out
	return
}

func pickOldestRuns(files []levelFile, n int, skip func(string) bool) (picked, remain []levelFile) {
	idx := make([]int, 0, len(files))
	for i := range files {
		idx = append(idx, i)
	}
	sort.Slice(idx, func(i, j int) bool { return files[idx[i]].id < files[idx[j]].id })

	pickedSet := make(map[int]struct{}, n)
	for _, i := range idx {
		if len(pickedSet) >= n {
			break
		}
		p := files[i].path
		if skip != nil && skip(p) {
			continue
		}
		pickedSet[i] = struct{}{}
	}

	picked = make([]levelFile, 0, len(pickedSet))
	remain = make([]levelFile, 0, len(files)-len(pickedSet))
	for i, f := range files {
		if _, ok := pickedSet[i]; ok {
			picked = append(picked, f)
		} else {
			remain = append(remain, f)
		}
	}
	return
}

// findRun 在 L1 及以上层中定位包含指定 key 的 run。
func findRun(runs []levelFile, key string) (string, bool) {
	i := sort.Search(len(runs), func(i int) bool {
		return runs[i].minKey > key
	}) - 1
	if i < 0 {
		return "", false
	}
	f := runs[i]
	if key >= f.minKey && key <= f.maxKey {
		return f.path, true
	}
	return "", false
}

// overlappedRuns 返回 runs 中所有与 [minK, maxK] 有交集的文件索引。
func overlappedRuns(files []levelFile, minK, maxK string) (idx []int) {
	start := sort.Search(len(files), func(i int) bool {
		return files[i].maxKey >= minK
	})
	for i := start; i < len(files); i++ {
		if files[i].minKey > maxK {
			break
		}
		idx = append(idx, i)
	}
	return idx
}

// writeSingleRun 把一段有序记录写成一个目标层 run。
func (d *DB) writeSingleRun(dstLevel int, entries []types.Entry) (levelFile, string, error) {
	if len(entries) == 0 {
		return levelFile{}, "", nil
	}

	d.mu.Lock()
	id := d.nextID
	d.nextID++
	dstDir := d.currentVersion().levels[dstLevel].dir
	d.mu.Unlock()

	name := fmt.Sprintf("%06d.sst", id)
	finalPath := filepath.Join(dstDir, name)
	tmpPath := finalPath + ".tmp"

	if err := sstable.WriteTable(tmpPath, entries); err != nil {
		_ = os.Remove(tmpPath)
		return levelFile{}, "", err
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return levelFile{}, "", err
	}
	if err := syncPathDir(finalPath); err != nil {
		_ = os.Remove(finalPath)
		return levelFile{}, "", err
	}

	return levelFile{
		id:     id,
		path:   finalPath,
		minKey: entries[0].Key,
		maxKey: entries[len(entries)-1].Key,
	}, finalPath, nil
}

// writeRuns 把一批有序记录按 runMaxEntries 切分并写成多个 run。
func (d *DB) writeRuns(dstLevel int, merged []types.Entry) ([]levelFile, []string, error) {
	if len(merged) == 0 {
		return nil, nil, nil
	}

	runMax := d.opt.levels[dstLevel].runMaxEntries
	if runMax <= 0 {
		return nil, nil, fmt.Errorf("db: invalid runMaxEntries for level %d", dstLevel)
	}
	newRuns := make([]levelFile, 0, (len(merged)+runMax-1)/runMax)
	created := make([]string, 0, cap(newRuns))

	for i := 0; i < len(merged); {
		j := i + runMax
		if j > len(merged) {
			j = len(merged)
		}
		chunk := merged[i:j]

		run, path, err := d.writeSingleRun(dstLevel, chunk)
		if err != nil {
			for _, p := range created {
				_ = os.Remove(p)
			}
			return nil, nil, err
		}

		created = append(created, path)
		newRuns = append(newRuns, run)
		i = j
	}

	return newRuns, created, nil
}
