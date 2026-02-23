package db

import (
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"monolithdb/internal/sstable"
)

func scanSSTables(sstDir string) (paths []string, maxID uint64, err error) {
	// 匹配这个目录下所有以 .sst 结尾的文件名
	glob := filepath.Join(sstDir, "*.sst")
	list, err := filepath.Glob(glob)
	if err != nil {
		return nil, 1, err
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

func scanL1(l1Dir string) (files []levelFile, maxID uint64, err error) {
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
		files = append(files, levelFile{path: p, minKey: minK, maxKey: maxK})
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].minKey < files[j].minKey
	})
	return files, maxID, nil
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

func (d *DB) findL1File(key string) (string, bool) {
	// 找到最后一个 minKey <= key 的文件
	i := sort.Search(len(d.l1), func(i int) bool {
		return d.l1[i].minKey > key
	}) - 1
	if i < 0 {
		return "", false
	}
	f := d.l1[i]
	if key >= f.minKey && key <= f.maxKey {
		return f.path, true
	}
	return "", false
}

// 返回 L1 中所有与给定 [minK, maxK] 范围存在交集（重叠）的文件索引。
func overlappedL1(files []levelFile, minK, maxK string) (idx []int) {
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
