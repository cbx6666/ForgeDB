package db

import "testing"

func TestDB_FlushAutoCompaction(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	// 连续 flush 多次，让 SST 数量超过阈值
	for i := 0; i < autoCompactThreshold; i++ {
		key := string(rune('A' + i))
		if err := d.Put(key, []byte("v")); err != nil {
			t.Fatal(err)
		}
		if err := d.Flush(); err != nil {
			t.Fatal(err)
		}
	}

	// 应该已经自动 compact 成 1 个 SST
	if len(d.sstables) != 1 {
		t.Fatalf("expected auto compaction to reduce sstables to 1, got %d", len(d.sstables))
	}

	// reopen 再验证
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	d2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()

	for i := 0; i < autoCompactThreshold; i++ {
		key := string(rune('A' + i))
		v, ok, err := d2.Get(key)
		if err != nil || !ok || string(v) != "v" {
			t.Fatalf("key %s lost after auto compaction: ok=%v v=%q err=%v", key, ok, string(v), err)
		}
	}
}
