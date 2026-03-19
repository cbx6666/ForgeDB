package monolithdb

import (
	"monolithdb/internal/db"
	"monolithdb/internal/types"
)

type WALSyncMode = db.WALSyncMode

const (
	WALSyncNever      = db.WALSyncNever
	WALSyncEveryBatch = db.WALSyncEveryBatch
	WALSyncInterval   = db.WALSyncInterval
)

type Options = db.Options

func DefaultOptions() Options {
	return db.DefaultOptions()
}

type Entry struct {
	Key       string
	Value     []byte
	Tombstone bool
}

type WriteBatch struct {
	inner db.WriteBatch
}

func (b *WriteBatch) Put(key string, value []byte) {
	if b == nil {
		return
	}
	b.inner.Put(key, value)
}

func (b *WriteBatch) Delete(key string) {
	if b == nil {
		return
	}
	b.inner.Delete(key)
}

func (b *WriteBatch) Len() int {
	if b == nil {
		return 0
	}
	return b.inner.Len()
}

type Iterator struct {
	inner *db.Iterator
}

func (it *Iterator) Valid() bool {
	return it != nil && it.inner != nil && it.inner.Valid()
}

func (it *Iterator) Entry() Entry {
	if !it.Valid() {
		return Entry{}
	}
	return convertEntry(it.inner.Entry())
}

func (it *Iterator) Next() error {
	if it == nil || it.inner == nil {
		return nil
	}
	return it.inner.Next()
}

func (it *Iterator) Close() error {
	if it == nil || it.inner == nil {
		return nil
	}
	return it.inner.Close()
}

type DB struct {
	inner *db.DB
}

func Open(dir string) (*DB, error) {
	inner, err := db.Open(dir)
	if err != nil {
		return nil, err
	}
	return &DB{inner: inner}, nil
}

func OpenWithOptions(dir string, opt Options) (*DB, error) {
	inner, err := db.OpenWithOptions(dir, opt)
	if err != nil {
		return nil, err
	}
	return &DB{inner: inner}, nil
}

func (d *DB) Close() error {
	if d == nil || d.inner == nil {
		return nil
	}
	return d.inner.Close()
}

func (d *DB) Put(key string, value []byte) error {
	return d.inner.Put(key, value)
}

func (d *DB) Get(key string) ([]byte, bool, error) {
	return d.inner.Get(key)
}

func (d *DB) Delete(key string) error {
	return d.inner.Delete(key)
}

func (d *DB) Write(batch *WriteBatch) error {
	if batch == nil {
		return nil
	}
	return d.inner.Write(&batch.inner)
}

func (d *DB) Flush() error {
	return d.inner.Flush()
}

func (d *DB) RequestCompaction() error {
	return d.inner.RequestCompaction()
}

func (d *DB) Scan(start, end string) ([]Entry, error) {
	entries, err := d.inner.Scan(start, end)
	if err != nil {
		return nil, err
	}
	out := make([]Entry, 0, len(entries))
	for _, ent := range entries {
		out = append(out, convertEntry(ent))
	}
	return out, nil
}

func (d *DB) NewIterator(start, end string) (*Iterator, error) {
	it, err := d.inner.NewIterator(start, end)
	if err != nil {
		return nil, err
	}
	return &Iterator{inner: it}, nil
}

func convertEntry(ent types.Entry) Entry {
	return Entry{
		Key:       ent.Key,
		Value:     clonePublicBytes(ent.Value),
		Tombstone: ent.Tombstone,
	}
}

func clonePublicBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	cp := make([]byte, len(b))
	copy(cp, b)
	return cp
}
