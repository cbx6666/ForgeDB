package sstable

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"

	"monolithdb/internal/types"
)

var	ErrCorruptSST = errors.New("sstable: corrupt")

const (
	magic uint32 = 0x46534442 // 'FSDB' = ForgeDB（仅用于识别文件）
)

// WriteTable 将有序 entries 写入 SSTable 文件。
func WriteTable(path string, entries []types.Entry) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	w := bufio.NewWriterSize(f, 64*1024)

	// 1) 写 header：magic + count
	if err := binary.Write(w, binary.LittleEndian, magic); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, uint32(len(entries))); err != nil {
		return err
	}

	// 2) 写 records
	for _, e := range entries {
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

	// 2) 顺序扫描 records
	target := key
	for i := uint32(0); i < count; i++ {
		var keyLen uint32
		var valLen uint32

		if err := binary.Read(r, binary.LittleEndian, &keyLen); err != nil {
			return nil, NotFound, ErrCorruptSST
		}
		if err := binary.Read(r, binary.LittleEndian, &valLen); err != nil {
			return nil, NotFound, ErrCorruptSST
		}

		tomb, err := r.ReadByte()
		if err != nil {
			return nil, NotFound, ErrCorruptSST
		}

		keyB := make([]byte, keyLen)
		if _, err := io.ReadFull(r, keyB); err != nil {
			return nil, NotFound, ErrCorruptSST
		}

		var valB []byte
		if valLen > 0 {
			valB = make([]byte, valLen)
			if _, err := io.ReadFull(r, valB); err != nil {
				return nil, NotFound, ErrCorruptSST
			}
		}

		k := string(keyB)
		if k == target {
			if tomb == 1 {
				return nil, Deleted, nil
			}
			return valB, Found, nil
		}
	}

	return nil, NotFound, nil
}
