package db

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"monolithdb/internal/manifest"
)

func recoverFromManifest(manPath string, sstDir string, opt Options) ([]level, uint64, error) {
	snap, err := manifest.LoadLastSnapshot(manPath)
	if err != nil {
		return nil, 0, err
	}
	if len(snap.Levels) != opt.numLevels {
		return nil, 0, fmt.Errorf("db: manifest levels mismatch: got %d want %d", len(snap.Levels), opt.numLevels)
	}

	// 先建立 levels
	levels := make([]level, 0, opt.numLevels)
	for i := 0; i < opt.numLevels; i++ {
		ldir := filepath.Join(sstDir, fmt.Sprintf("l%d", i))
		if err := os.MkdirAll(ldir, 0o755); err != nil {
			return nil, 0, err
		}
		levels = append(levels, level{id: i, dir: ldir})
	}

	// L0: 直接恢复顺序（newest-first）
	levels[0].l0Paths = append([]string(nil), snap.Levels[0]...)

	// L>=1: 用 path 补齐 run 的 min/max
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
