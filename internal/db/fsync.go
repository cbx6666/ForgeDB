package db

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"
)

var syncDirFn = syncDirIfSupported

// syncDirIfSupported 尽力持久化目录项变更。
// 这用于覆盖 rename/remove 之后父目录元数据的落盘。
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

// removeFileAndSync 删除文件后同步其父目录。
// Windows 上如果遇到短暂的共享冲突，会做几次短重试。
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

// isTransientRemoveErr 判断 Windows 上的临时共享冲突，
// 这类错误短暂重试后往往可以恢复。
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
