package db

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
)

var syncDirFn = syncDirIfSupported

// syncDirIfSupported best-effort 地持久化目录项变更。
// 这用于覆盖 rename/remove 之后的父目录元数据落盘。
// Windows 上目录 fsync 语义不稳定，这里直接跳过。
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

// isDirSyncUnsupported 用于识别“当前平台/文件系统不支持目录 sync”的错误。
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

// syncPathDir 对文件所在目录做一次同步，用于在 rename 后固化目录项变更。
func syncPathDir(path string) error {
	return syncDirFn(filepath.Dir(path))
}

// removeFileAndSync 删除文件后同步其父目录，避免目录项变更只停留在页缓存里。
func removeFileAndSync(path string) error {
	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return syncPathDir(path)
}
