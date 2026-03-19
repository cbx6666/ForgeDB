package sstable

import (
	"encoding/binary"
	"io"
	"os"
)

// footer 布局：
// [indexStartOffset(uint64)][bloomStartOffset(uint64)][metaStartOffset(uint64)]
const footerSize = 24

// loadFooter 读取并校验 footer，返回 index、bloom、meta 三个区域的起始偏移。
// 文件布局约束：
//
//	header(8) ... records ... index ... bloom ... meta ... footer(24)
//	indexStartOffset >= headerSize
//	indexStartOffset < bloomStartOffset
//	bloomStartOffset < metaStartOffset
//	metaStartOffset < footerStart
func loadFooter(f *os.File, fileSize int64) (indexStartOffset, bloomStartOffset, metaStartOffset uint64, err error) {
	if fileSize < int64(headerSize+footerSize) {
		return 0, 0, 0, ErrCorruptSST
	}

	// footerStart 是 footer 起始位置，也就是 meta 区的结束位置。
	footerStart := uint64(fileSize) - uint64(footerSize)

	// 跳到文件尾部读取 footer 中的三个 offset。
	if _, err := f.Seek(-footerSize, io.SeekEnd); err != nil {
		return 0, 0, 0, err
	}
	if err := binary.Read(f, binary.LittleEndian, &indexStartOffset); err != nil {
		return 0, 0, 0, ErrCorruptSST
	}
	if err := binary.Read(f, binary.LittleEndian, &bloomStartOffset); err != nil {
		return 0, 0, 0, ErrCorruptSST
	}
	if err := binary.Read(f, binary.LittleEndian, &metaStartOffset); err != nil {
		return 0, 0, 0, ErrCorruptSST
	}

	// 校验三个区域的边界顺序是否合理。
	if indexStartOffset < uint64(headerSize) || indexStartOffset >= footerStart {
		return 0, 0, 0, ErrCorruptSST
	}
	if bloomStartOffset <= indexStartOffset || bloomStartOffset >= footerStart {
		return 0, 0, 0, ErrCorruptSST
	}
	if metaStartOffset <= bloomStartOffset || metaStartOffset >= footerStart {
		return 0, 0, 0, ErrCorruptSST
	}

	return indexStartOffset, bloomStartOffset, metaStartOffset, nil
}
