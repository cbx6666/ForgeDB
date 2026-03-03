package db

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// cleanupTmpFiles removes any "*.tmp" files under sstDir.
// Safe because tmp files are never "committed" (we always rename tmp->final atomically).
func cleanupTmpFiles(sstDir string) error {
	return filepath.WalkDir(sstDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasSuffix(d.Name(), ".tmp") {
			_ = os.Remove(path)
		}
		return nil
	})
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

// sanityCheckSSTDir cleans tmp files and checks for orphan SSTs.
// If orphan exists, returns an error with a short list for debugging.
func sanityCheckSSTDir(sstDir string, levels []level) ([]string, error) {
	// 1) clean tmp
	if err := cleanupTmpFiles(sstDir); err != nil {
		return nil, err
	}

	// 2) detect orphan sst
	all, err := listAllSSTFiles(sstDir)
	if err != nil {
		return nil, err
	}
	ref := referencedSSTSet(levels)
	orphan := findOrphanSST(all, ref)
	if len(orphan) == 0 {
		return nil, nil
	}
	return orphan, nil
}
