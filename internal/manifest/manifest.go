package manifest

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"
)

var ErrNoSnapshot = errors.New("manifest: no snapshot")

type Manifest struct {
	path string
	f    *os.File
	enc  *json.Encoder
}

type Snapshot struct {
	NextFile uint64     `json:"nextFile"`
	Levels   [][]string `json:"levels"`
}

// Open opens MANIFEST in append mode (create if not exists).
func Open(path string) (*Manifest, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &Manifest{
		path: path,
		f:    f,
		enc:  json.NewEncoder(f),
	}, nil
}

func (m *Manifest) Close() error {
	if m == nil || m.f == nil {
		return nil
	}
	return m.f.Close()
}

// WriteSnapshot appends a full snapshot record and fsyncs it.
func (m *Manifest) WriteSnapshot(next uint64, levels [][]string) error {
	if m == nil || m.f == nil {
		return errors.New("manifest: closed")
	}
	s := Snapshot{
		NextFile: next,
		Levels:   levels,
	}
	if err := m.enc.Encode(&s); err != nil {
		return err
	}
	return m.f.Sync()
}

// CheckpointLatest rewrites MANIFEST to contain only one latest snapshot.
// It uses tmp + rename, and then reopens the manifest for further appends.
func (m *Manifest) CheckpointLatest() error {
	if m == nil || m.f == nil {
		return errors.New("manifest: closed")
	}

	// 1) 从当前文件中读出最后一个 snapshot
	last, err := LoadLastSnapshot(m.path)
	if err != nil {
		return err
	}

	// 2) 写入 tmp（只写一条）
	tmp := m.path + ".tmp"
	tf, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(tf)
	if err := enc.Encode(last); err != nil {
		_ = tf.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := tf.Sync(); err != nil {
		_ = tf.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := tf.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}

	// 3) 关闭当前 fd，rename 覆盖旧 manifest
	_ = m.f.Close()
	m.f = nil
	m.enc = nil

	if err := os.Rename(tmp, m.path); err != nil {
		_ = os.Remove(tmp)
		return err
	}

	// 4) 重新以 append 模式打开，并恢复 encoder
	f, err := os.OpenFile(m.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	m.f = f
	m.enc = json.NewEncoder(f)
	return nil
}

// LoadLastSnapshot scans MANIFEST and returns the last valid snapshot.
func LoadLastSnapshot(path string) (*Snapshot, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var last *Snapshot
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024), 10*1024*1024) // 允许单行最多 10MB

	// sc.Scan() 扫描一行文件
	for sc.Scan() {
		line := sc.Bytes() // 返回字节切片
		// ignore blank lines
		if len(line) == 0 {
			continue
		}
		var s Snapshot
		if err := json.Unmarshal(line, &s); err != nil { // 反序列化
			continue
		}
		last = &s
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if last == nil {
		return nil, ErrNoSnapshot
	}
	return last, nil
}
