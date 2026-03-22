package db

import (
	"sync"

	"monolithdb/internal/manifest"
	"monolithdb/internal/memtable"
	"monolithdb/internal/sstable"
	"monolithdb/internal/wal"
)

// db_state.go 负责维护 DB 的核心状态、层级元数据和轻量读视图。

type levelFile struct {
	id     uint64
	path   string
	minKey string
	maxKey string
}

type level struct {
	id  int
	dir string

	// L0 使用 newest-first，并允许 key range 重叠。
	l0Paths []string
	// L1 及以上按 minKey 升序组织，并保证不重叠。
	runs []levelFile
}

// Version 表示当前对外可见的一组层级视图。
type Version struct {
	levels []level
}

// readView 表示一次读操作抓取到的版本快照。
type readView struct {
	version *Version
}

type DB struct {
	mem *memtable.MemTable
	imm *memtable.MemTable
	wal *wal.WAL

	man            *manifest.Manifest
	manifestWrites int

	dir        string
	walPath    string
	immWalPath string // 冻结 memtable 对应的 flush WAL 路径。
	sstDir     string

	current *Version
	opt     Options
	nextID  uint64
	cache   *sstable.TableCache

	// 写请求先进入队列，再由 writeLoop 统一做 group commit。
	writeMu       sync.RWMutex
	writeCh       chan *writeReq
	writeClosed   bool
	syncWAL       func() error
	syncStop      chan struct{}
	autoFlushCh   chan struct{}
	autoFlushStop chan struct{}

	// 跟踪“已写入 WAL”与“已 fsync”的边界。
	writeSeq  uint64
	syncedSeq uint64

	// 共享后台状态。
	mu   sync.RWMutex
	cond *sync.Cond
	wg   sync.WaitGroup

	closing       bool
	compactionReq bool
	compacting    bool
	bgErr         error
	pending       map[string]struct{} // path -> pending
}

// currentVersion 返回当前读视图使用的版本对象。
func (d *DB) currentVersion() *Version {
	if d.current == nil {
		return &Version{}
	}
	return d.current
}
