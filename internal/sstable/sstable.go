package sstable

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"

	"monolithdb/internal/types"
)

var ErrCorruptSST = errors.New("sstable: corrupt")

const (
	magic uint32 = 0x46534442 // 'FSDB' = ForgeDB（仅用于识别文件）
)

type countWriter struct {
	w *bufio.Writer
	n uint64
}

func newCountWriter(f *os.File) *countWriter {
	return &countWriter{w: bufio.NewWriterSize(f, 64*1024)}
}

func (cw *countWriter) Write(p []byte) (int, error) {
	n, err := cw.w.Write(p)
	cw.n += uint64(n)
	return n, err
}

func (cw *countWriter) WriteByte(b byte) error {
	if err := cw.w.WriteByte(b); err != nil {
		return err
	}
	cw.n++
	return nil
}

func (cw *countWriter) Flush() error { return cw.w.Flush() }

// WriteTable 将有序 entries 写入 SSTable 文件。
func WriteTable(path string, entries []types.Entry) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	w := newCountWriter(f)

	// 1) 写 header：magic + count
	if err := binary.Write(w, binary.LittleEndian, magic); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, uint32(len(entries))); err != nil {
		return err
	}

	// 2) 写 records 和索引
	var idx []indexEntry

	for i, e := range entries {
		recOff := w.n

		if i%indexStride == 0 {
			idx = append(idx, indexEntry{key: e.Key, offset: recOff})
		}

		keyB := []byte(e.Key)
		valB := e.Value

		if err := binary.Write(w, binary.LittleEndian, uint32(len(keyB))); err != nil {
			return err
		}
		if err := binary.Write(w, binary.LittleEndian, uint32(len(valB))); err != nil {
			return err
		}

		var tomb byte = 0
		if e.Tombstone {
			tomb = 1
		}
		if err := w.WriteByte(tomb); err != nil {
			return err
		}

		if _, err := w.Write(keyB); err != nil {
			return err
		}
		if len(valB) > 0 {
			if _, err := w.Write(valB); err != nil {
				return err
			}
		}
	}

	// 写索引
	indexStartOffset := w.n

	// indexCount
	if err := binary.Write(w, binary.LittleEndian, uint32(len(idx))); err != nil {
		return err
	}

	// index entries: [keyLen][keyBytes][recordOffset(uint64)]
	for _, it := range idx {
		kb := []byte(it.key)
		if err := binary.Write(w, binary.LittleEndian, uint32(len(kb))); err != nil {
			return err
		}
		if _, err := w.Write(kb); err != nil {
			return err
		}
		if err := binary.Write(w, binary.LittleEndian, it.offset); err != nil {
			return err
		}
	}

	// footer: indexStartOffset (最后 8 字节)
	if err := binary.Write(w, binary.LittleEndian, indexStartOffset); err != nil {
		return err
	}

	return w.Flush()
}

// Get 从 SSTable 文件中查找 key。
func Get(path string, key string) ([]byte, GetResult, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, NotFound, err
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, 64*1024)

	// 1) 读 header
	var m uint32
	if err := binary.Read(r, binary.LittleEndian, &m); err != nil {
		return nil, NotFound, err
	}
	if m != magic {
		return nil, NotFound, ErrCorruptSST
	}

	var count uint32
	if err := binary.Read(r, binary.LittleEndian, &count); err != nil {
		return nil, NotFound, ErrCorruptSST
	}

	// 2) 读取索引
	st, err := f.Stat()
	if err != nil {
		return nil, NotFound, err
	}
	fileSize := st.Size()

	entries, indexStartOffset, err := loadIndex(f, fileSize)
	if err != nil {
		return nil, NotFound, err
	}

	start, end := pickScanRange(entries, indexStartOffset, key)
	if end <= start {
		return nil, NotFound, ErrCorruptSST
	}

	section := io.NewSectionReader(f, int64(start), int64(end-start))
	sr := bufio.NewReaderSize(section, 64*1024)

	// 3) 根据索引查找
	for {
		var keyLen uint32
		var valLen uint32

		if err := binary.Read(sr, binary.LittleEndian, &keyLen); err != nil {
			// 区间读完就结束：没找到
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil, NotFound, nil
			}
			return nil, NotFound, ErrCorruptSST
		}
		if err := binary.Read(sr, binary.LittleEndian, &valLen); err != nil {
			return nil, NotFound, ErrCorruptSST
		}

		tomb, err := sr.ReadByte()
		if err != nil {
			return nil, NotFound, ErrCorruptSST
		}

		keyB := make([]byte, keyLen)
		if _, err := io.ReadFull(sr, keyB); err != nil {
			return nil, NotFound, ErrCorruptSST
		}

		var valB []byte
		if valLen > 0 {
			valB = make([]byte, valLen)
			if _, err := io.ReadFull(sr, valB); err != nil {
				return nil, NotFound, ErrCorruptSST
			}
		}

		k := string(keyB)
		if k == key {
			if tomb == 1 {
				return nil, Deleted, nil
			}
			return valB, Found, nil
		}
		if k > key {
			return nil, NotFound, nil
		}
	}
}
