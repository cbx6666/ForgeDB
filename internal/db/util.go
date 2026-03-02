package db

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"monolithdb/internal/sstable"
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

func pickOldestL0(l0 []string, n int) (picked, remain []string) {
	if len(l0) <= n {
		return l0, nil
	}
	picked = append([]string(nil), l0[len(l0)-n:]...)
	remain = append([]string(nil), l0[:len(l0)-n]...)
	return picked, remain
}

func pickOldestRuns(files []levelFile, n int) (picked, remain []levelFile) {
	if len(files) <= n {
		return append([]levelFile(nil), files...), nil
	}

	// 找 id 最小的 n 个
	idx := make([]int, len(files))
	for i := range files {
		idx[i] = i
	}

	sort.Slice(idx, func(i, j int) bool {
		return files[idx[i]].id < files[idx[j]].id
	})

	pickedSet := make(map[int]struct{}, n)
	for i := 0; i < n; i++ {
		pickedSet[idx[i]] = struct{}{}
	}

	picked = make([]levelFile, 0, n)
	remain = make([]levelFile, 0, len(files)-n)

	// 保留文件原始顺序
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
