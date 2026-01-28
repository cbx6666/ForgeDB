package types

// KV 记录
type Entry struct {
	Key string
	Value []byte
	Tombstone bool // 删除标记
}
