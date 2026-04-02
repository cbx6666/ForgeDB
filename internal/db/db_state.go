package db

import (
	"sync"

	"monolithdb/internal/manifest"
	"monolithdb/internal/memtable"
	"monolithdb/internal/sstable"
	"monolithdb/internal/types"
	"monolithdb/internal/wal"
)

// db_state.go 负责维护 DB 的核心状态、层级元数据和轻量读视图。

// levelFile 表示某一层中的单个 SST run。
type levelFile struct {
	id     uint64
	path   string
	minKey string
	maxKey string
}

// level 表示某一层的文件组织方式。
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

// immutableMem 表示一份已经冻结、只读的 memtable。
type immutableMem struct {
	// gen 是这份 immutable memtable 对应的 flush 代际号。
	gen uint64
	// mem 是冻结后的只读内存表。
	mem *memtable.MemTable
	// walPath 是这份冻结表对应的 WAL 路径。
	walPath string
}

// pointReadView 表示一次点查抓取到的只读快照。
type pointReadView struct {
	// memEntry / memFound 用于保存 active memtable 中的点查结果快照。
	memEntry types.Entry
	memFound bool
	// imms 是当前 immutable memtable 队列快照，按 flush 顺序排列。
	imms []*immutableMem
	// version 是当前对外可见的 SST 视图。
	version *Version
}

// scanReadView 表示一次范围读抓取到的只读快照。
type scanReadView struct {
	// memEntries 用于保存 active memtable 中预先物化出来的范围结果。
	memEntries []types.Entry
	// imms 是当前 immutable memtable 队列快照，按 flush 顺序排列。
	imms []*immutableMem
	// version 是当前对外可见的 SST 视图。
	version *Version
}

// writeState 收拢写入提交、WAL 同步和写入边界统计。
type writeState struct {
	// mu 只保护写入口关闭状态，避免提交方和 Close 竞争写通道。
	mu sync.RWMutex
	// ch 是前台写请求投递到 writeLoop 的通道。
	ch chan *writeReq
	// closed 表示写通道已经关闭，后续不再接受新写入。
	closed bool

	// syncWAL 负责对当前活跃 WAL 做 fsync。
	syncWAL func() error
	// syncStop 用于停止周期性 WAL 同步协程；仅 interval 模式启用。
	syncStop chan struct{}

	// seq / syncedSeq 分别表示“已经写入 WAL”的边界和“已经 fsync”的边界。
	seq       uint64
	syncedSeq uint64
}

// flushState 收拢 flush 的触发、等待和任务代际状态。
type flushState struct {
	// cond 用于协调 Flush 调用方、autoFlushLoop 和后台 flushLoop。
	// 等待谓词围绕 req、running、doneGen 和 DB 关闭状态展开。
	cond *sync.Cond

	// autoCh / autoStop 控制自动 flush 触发协程。
	autoCh   chan struct{}
	autoStop chan struct{}

	// req 表示存在待处理的 flush 请求。
	req bool
	// running 表示后台 flushLoop 正在执行一轮 flush。
	running bool

	// gen 表示最近一次 enqueue 成功创建的 flush 代际号。
	gen uint64
	// doneGen 表示最近一次 install 完成的 flush 代际号。
	doneGen uint64
	// job 表示当前已经 start、但尚未 install 完成的 flush 任务。
	job *flushJob
}

// compactionState 收拢 compaction 调度与文件占用状态。
type compactionState struct {
	// cond 只用于唤醒 compactionLoop。
	// 等待谓词是 DB 进入 closing，或 compactionReq 变为真。
	cond *sync.Cond

	compactionReq bool
	pendingPaths  map[string]struct{} // path -> pending
}

// DB 表示数据库实例的完整运行态。
type DB struct {
	// mem 是当前仍可写的 active memtable。
	mem *memtable.MemTable
	// imms 保存所有等待 flush 的 immutable memtable，按入队顺序排列。
	imms []*immutableMem
	// wal 是当前 active memtable 对应的 WAL。
	wal *wal.WAL

	man            *manifest.Manifest
	manifestWrites int

	dir     string
	walPath string
	sstDir  string

	current *Version
	opt     Options
	nextID  uint64
	cache   *sstable.TableCache

	// mu 保护 mem / imms / version 以及后台状态转换。
	mu sync.RWMutex
	wg sync.WaitGroup

	// closing 表示 DB 已进入关闭流程，前台不再接受新的运行时操作。
	closing bool
	// bgErr 记录后台路径上的首个错误；一旦设置，后续操作会快速失败。
	bgErr error

	write      writeState
	flush      flushState
	compaction compactionState
}

// currentVersion 返回当前读视图使用的版本对象。
func (d *DB) currentVersion() *Version {
	if d.current == nil {
		return &Version{}
	}
	return d.current
}
