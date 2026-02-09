package sstable

type GetResult uint8

const (
	NotFound GetResult = iota
	Found
	Deleted
)
