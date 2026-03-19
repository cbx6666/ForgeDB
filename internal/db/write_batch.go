package db

// WriteBatch 用于收集多条要一起提交的变更。
// 零值可直接使用。
type WriteBatch struct {
	ops []batchOp
}

func (b *WriteBatch) Put(key string, value []byte) {
	if b == nil {
		return
	}
	b.ops = append(b.ops, batchOp{
		op:    writeOpPut,
		key:   key,
		value: cloneWriteValue(value),
	})
}

func (b *WriteBatch) Delete(key string) {
	if b == nil {
		return
	}
	b.ops = append(b.ops, batchOp{
		op:  writeOpDelete,
		key: key,
	})
}

func (b *WriteBatch) Len() int {
	if b == nil {
		return 0
	}
	return len(b.ops)
}
