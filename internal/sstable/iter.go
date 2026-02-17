package sstable

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"

	"monolithdb/internal/types"
)

type Iter struct {
	f     *os.File
	r     *bufio.Reader
	left  uint32 // 剩余 records 数量
	cur   types.Entry
	valid bool
}

func NewIter(path string) (*Iter, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	r := bufio.NewReaderSize(f, 64*1024)

	// header: magic + count
	var m uint32
	if err := binary.Read(r, binary.LittleEndian, &m); err != nil {
		_ = f.Close()
		return nil, err
	}
	if m != magic {
		_ = f.Close()
		return nil, ErrCorruptSST
	}

	var count uint32
	if err := binary.Read(r, binary.LittleEndian, &count); err != nil {
		_ = f.Close()
		return nil, ErrCorruptSST
	}

	it := &Iter{f: f, r: r, left: count}
	if err := it.readNext(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return it, nil
}

func (it *Iter) Valid() bool { return it.valid }

func (it *Iter) Entry() types.Entry {
	return it.cur
}

func (it *Iter) Next() error {
	return it.readNext()
}

func (it *Iter) Close() error {
	if it.f != nil {
		return it.f.Close()
	}
	return nil
}

func (it *Iter) readNext() error {
	if it.left == 0 {
		it.valid = false
		return nil
	}

	var keyLen uint32
	var valLen uint32

	if err := binary.Read(it.r, binary.LittleEndian, &keyLen); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return ErrCorruptSST
		}
		return err
	}
	if err := binary.Read(it.r, binary.LittleEndian, &valLen); err != nil {
		return ErrCorruptSST
	}

	tomb, err := it.r.ReadByte()
	if err != nil {
		return ErrCorruptSST
	}

	keyB := make([]byte, keyLen)
	if _, err := io.ReadFull(it.r, keyB); err != nil {
		return ErrCorruptSST
	}

	var valB []byte
	if valLen > 0 {
		valB = make([]byte, valLen)
		if _, err := io.ReadFull(it.r, valB); err != nil {
			return ErrCorruptSST
		}
	}

	it.cur = types.Entry{
		Key:       string(keyB),
		Value:     valB,
		Tombstone: tomb == 1,
	}
	it.left--
	it.valid = true
	return nil
}
