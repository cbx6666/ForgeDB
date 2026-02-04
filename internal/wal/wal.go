package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
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

// Record 表示 WAL 中的一条记录。
type Record struct {
	Op    byte
	Key   string
	Value []byte
}

const (
	opPut    byte = 0
	opDelete byte = 1
)

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
		// 防止还有残留数据在内存里没写出去
		_ = w.buf.Flush()
	}
	if w.f != nil {
		return w.f.Close()
	}

	return nil
}

// AppendPut 追加一条 Put 记录到 WAL 文件。
// 记录格式：| op(1B) | keyLen(uint32) | valLen(uint32) | key bytes | val bytes |
func (w *WAL) AppendPut(key string, value []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 1) op
	if err := w.buf.WriteByte(opPut); err != nil {
		return err
	}

	// 2) keyLen / valLen
	keyB := []byte(key)
	if err := binary.Write(w.buf, binary.LittleEndian, uint32(len(keyB))); err != nil {
		return err
	}
	if err := binary.Write(w.buf, binary.LittleEndian, uint32(len(value))); err != nil {
		return err
	}

	// 3) key bytes / value bytes
	if _, err := w.buf.Write(keyB); err != nil {
		return err
	}
	if len(value) > 0 {
		if _, err := w.buf.Write(value); err != nil {
			return err
		}
	}

	return w.buf.Flush()
}

// AppendDelete 追加一条 Delete 记录到 WAL 文件。
// 记录格式：| op(1B) | keyLen(uint32) | valLen(uint32=0) | key bytes |
func (w *WAL) AppendDelete(key string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 1) op
	if err := w.buf.WriteByte(opDelete); err != nil {
		return err
	}

	// 2) keyLen / valLen(=0)
	keyB := []byte(key)
	if err := binary.Write(w.buf, binary.LittleEndian, uint32(len(keyB))); err != nil {
		return err
	}
	if err := binary.Write(w.buf, binary.LittleEndian, uint32(0)); err != nil {
		return err
	}

	// 3) key bytes
	if _, err := w.buf.Write(keyB); err != nil {
		return err
	}

	return w.buf.Flush()
}

var ErrCorruptWAL = errors.New("wal: corrupt record")

// Replay 读取整个 WAL 文件并解析成 Record 列表。
func Replay(path string) ([]Record, error) {
	f, err := os.Open(path)
	if err != nil {
		// WAL 不存在就当作空
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, 64*1024)
	var out []Record

	for {
		// 1) 读 op
		op, err := r.ReadByte()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return out, nil
			}
			return nil, err
		}

		// 2) 读 keyLen / valLen
		var keyLen uint32
		var valLen uint32
		if err := binary.Read(r, binary.LittleEndian, &keyLen); err != nil {
			return nil, ErrCorruptWAL
		}
		if err := binary.Read(r, binary.LittleEndian, &valLen); err != nil {
			return nil, ErrCorruptWAL
		}

		// 3) 读 key bytes
		keyB := make([]byte, keyLen)
		// io.ReadFull(r,keyB)：必须把 keyB 填满，否则就返回错误
		if _, err := io.ReadFull(r, keyB); err != nil {
			return nil, ErrCorruptWAL
		}

		// 4) 读 value bytes（delete 的 valLen=0）
		var valB []byte
		if valLen > 0 {
			valB = make([]byte, valLen)
			if _, err = io.ReadFull(r, valB); err != nil {
				return nil, ErrCorruptWAL
			}
		}

		// 5) 简单校验 op
		if op != opPut && op != opDelete {
			return nil, ErrCorruptWAL
		}

		out = append(out, Record{
			Op:    op,
			Key:   string(keyB),
			Value: valB,
		})
	}
}
