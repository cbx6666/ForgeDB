package sstable

import (
	"encoding/binary"
	"io"
	"os"
)

// loadBounds 从 meta 区中读取持久化保存的最小/最大 key。
func loadBounds(f *os.File, fileSize int64) (string, string, error) {
	_, _, metaStartOffset, err := loadFooter(f, fileSize)
	if err != nil {
		return "", "", err
	}

	footerStart := uint64(fileSize) - uint64(footerSize)
	if _, err := f.Seek(int64(metaStartOffset), io.SeekStart); err != nil {
		return "", "", err
	}

	var minLen uint32
	if err := binary.Read(f, binary.LittleEndian, &minLen); err != nil {
		return "", "", ErrCorruptSST
	}
	if metaStartOffset+4+uint64(minLen)+4 > footerStart {
		return "", "", ErrCorruptSST
	}

	minKey := make([]byte, minLen)
	if _, err := io.ReadFull(f, minKey); err != nil {
		return "", "", ErrCorruptSST
	}

	var maxLen uint32
	if err := binary.Read(f, binary.LittleEndian, &maxLen); err != nil {
		return "", "", ErrCorruptSST
	}
	if metaStartOffset+4+uint64(minLen)+4+uint64(maxLen) != footerStart {
		return "", "", ErrCorruptSST
	}

	maxKey := make([]byte, maxLen)
	if _, err := io.ReadFull(f, maxKey); err != nil {
		return "", "", ErrCorruptSST
	}

	return string(minKey), string(maxKey), nil
}

// LoadBounds 读取指定 SST 中持久化保存的最小/最大 key。
func LoadBounds(path string) (string, string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", "", err
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return "", "", err
	}
	return loadBounds(f, st.Size())
}
