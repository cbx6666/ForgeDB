package sstable

import (
	"encoding/binary"
	"io"
	"os"
)

// footer 布局：
// [indexStartOffset(uint64)][bloomStartOffset(uint64)]
const footerSize = 16

// loadFooter 读取并校验 footer，返回 indexStartOffset 与 bloomStartOffset。
// 约束（对应你的文件布局）：
//
//	header(8) ... records ... index ... bloom ... footer(16)
//	indexStartOffset >= headerSize
//	indexStartOffset < bloomStartOffset
//	bloomStartOffset < footerStart
func loadFooter(f *os.File, fileSize int64) (indexStartOffset, bloomStartOffset uint64, err error) {
	if fileSize < int64(headerSize+footerSize) {
		return 0, 0, ErrCorruptSST
	}

	// footerStart 是 footer 起始位置（也是 bloom 区的 end）
	footerStart := uint64(fileSize) - uint64(footerSize)

	// seek 到 footer 并读取两个 offset
	if _, err := f.Seek(-footerSize, io.SeekEnd); err != nil {
		return 0, 0, err
	}
	if err := binary.Read(f, binary.LittleEndian, &indexStartOffset); err != nil {
		return 0, 0, ErrCorruptSST
	}
	if err := binary.Read(f, binary.LittleEndian, &bloomStartOffset); err != nil {
		return 0, 0, ErrCorruptSST
	}

	// 校验 offset 合法性
	if indexStartOffset < uint64(headerSize) || indexStartOffset >= footerStart {
		return 0, 0, ErrCorruptSST
	}
	if bloomStartOffset <= indexStartOffset || bloomStartOffset >= footerStart {
		return 0, 0, ErrCorruptSST
	}

	return indexStartOffset, bloomStartOffset, nil
}
