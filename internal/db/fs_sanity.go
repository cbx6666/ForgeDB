package db

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// cleanupTmpFiles removes any "*.tmp" files under sstDir.
// Safe because tmp files are never committed; they become visible only after rename.
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

// listAllSSTFiles collects all "*.sst" files under sstDir.
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

// referencedSSTSet builds a set of SST paths referenced by current in-memory levels.
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

// findOrphanSST returns SST paths that exist on disk but are not referenced by manifest-derived levels.
func findOrphanSST(all []string, ref map[string]struct{}) []string {
	var orphan []string
	for _, p := range all {
		if _, ok := ref[p]; !ok {
			orphan = append(orphan, p)
		}
	}
	return orphan
}

// sanityCheckSSTDir cleans tmp files and removes orphan SSTs.
// The manifest is the source of truth, so unreferenced SSTs are discarded.
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
