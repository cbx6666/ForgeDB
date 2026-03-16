package db

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"monolithdb/internal/sstable"
	"monolithdb/internal/types"
)

func scanLevel0(sstDir string) (paths []string, maxID uint64, err error) {
	// 匹配这个目录下所有以 .sst 结尾的文件名
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

	// 内存里用 newest-first，所以反转
	for i, j := 0, len(list)-1; i < j; i, j = i+1, j-1 {
		list[i], list[j] = list[j], list[i]
	}

	return list, maxID, nil
}

func scanLevelN(l1Dir string) (files []levelFile, maxID uint64, err error) {
	glob := filepath.Join(l1Dir, "*.sst")
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

func scanAllLevels(sstDir string, opt Options) ([]level, uint64, error) {
	levels := make([]level, 0, opt.numLevels)
	var maxID uint64 = 0

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

	nextID := maxID + 1
	return levels, nextID, nil
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
	base := filepath.Base(path)                // 000001.sst
	name := strings.TrimSuffix(base, ".sst")   // 000001
	id, err := strconv.ParseUint(name, 10, 64) // 把字符串解析成无符号整数，base：进制，bitSize：目标位宽
	if err != nil {
		return 0, false
	}
	return id, true
}

func sstKeyRange(path string) (min, max string, err error) {
	it, err := sstable.NewIter(path)
	if err != nil {
		return "", "", err
	}
	defer it.Close()

	if !it.Valid() {
		return "", "", nil
	}
	min = it.Entry().Key
	max = min
	for it.Valid() {
		max = it.Entry().Key
		if err := it.Next(); err != nil {
			return "", "", err
		}
	}
	return min, max, nil
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

// 返回 runs 中所有与 [minK, maxK] 有交集的文件索引（适用于 L>=1 的 non-overlap runs）。
func overlappedRuns(files []levelFile, minK, maxK string) (idx []int) {
	// 找第一个 maxKey >= minK 的位置
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

		d.mu.Lock()
		id := d.nextID
		d.nextID++
		dstDir := d.currentVersion().levels[dstLevel].dir
		d.mu.Unlock()

		name := fmt.Sprintf("%06d.sst", id)
		finalPath := filepath.Join(dstDir, name)
		tmpPath := finalPath + ".tmp"

		if err := sstable.WriteTable(tmpPath, chunk); err != nil {
			_ = os.Remove(tmpPath)
			// 清理已创建的 run
			for _, p := range created {
				_ = os.Remove(p)
			}
			return nil, nil, err
		}
		if err := os.Rename(tmpPath, finalPath); err != nil {
			_ = os.Remove(tmpPath)
			for _, p := range created {
				_ = os.Remove(p)
			}
			return nil, nil, err
		}

		created = append(created, finalPath)
		newRuns = append(newRuns, levelFile{
			id:     id,
			path:   finalPath,
			minKey: chunk[0].Key,
			maxKey: chunk[len(chunk)-1].Key,
		})

		i = j
	}

	return newRuns, created, nil
}
