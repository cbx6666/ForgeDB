package sstable

import "errors"

type GetResult uint8

var ErrCorruptSST = errors.New("sstable: corrupt")

const (
	NotFound GetResult = iota
	Found
	Deleted
)
