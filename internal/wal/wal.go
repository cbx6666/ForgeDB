package wal

import (
	"bufio"
	"os"
	"sync"
)

// WAL 是预写日志（Write-Ahead Log）。
// 作用：写入先追加到日志，崩溃后可通过回放恢复内存状态。
type WAL struct {
	mu  sync.Mutex
	f   *os.File
	buf *bufio.Writer
}

// Open 打开或创建 WAL 文件，准备追加写。
func Open(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)

	if err != nil {
		return nil, err
	}

	return &WAL{
		f:   f,
		buf: bufio.NewWriterSize(f, 64*1024),
	}, nil
}

// Close 关闭 WAL（会先 Flush 缓冲区）。
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.buf != nil {
		// 防止还有残留数据在内存里没写出去。
		_ = w.buf.Flush()
	}
	if w.f != nil {
		return w.f.Close()
	}

	return nil
}
