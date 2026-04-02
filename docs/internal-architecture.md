# Internal Project Summary

本文档按 `internal/*` 目录总结当前项目的内部实现。目标不是写设计口号，而是把每个目录、每个文件、每个主要函数、每组测试在做什么说清楚，方便后续维护、重构和故障定位。

## 总览

当前项目是一个单机 LSM 风格 KV 存储引擎，核心路径是：

1. 前台写入进入 writer queue。
2. `writeLoop` 做 group commit。
3. 先写 WAL，再写 active memtable。
4. active memtable 达到条件后冻结为 immutable memtable，进入 flush 队列。
5. `flushLoop` 以单 worker 方式把 immutable memtable 刷成 SST。
6. `compactionLoop` 以单 worker 方式执行层级压缩。
7. 读路径按 `mem -> imms -> SST` 顺序读取。
8. `Version + MANIFEST` 负责维护当前层级布局与恢复入口。

当前实现的重要边界：

- 单 writer。
- immutable flush queue。
- 单 worker flush。
- 单 worker compaction。
- 读路径已开始快照化。
- 旧 flush 单文件格式兼容已移除，新模型只认 `forge.flush.<gen>.wal`。

---

## `internal/types`

### 目录职责

提供跨模块共享的基础数据结构，目前只有逻辑记录类型。

### 文件：`entry.go`

#### 结构体

- `types.Entry`
  - `Key string`
  - `Value []byte`
  - `Tombstone bool`
  - 作用：统一表示 memtable、SST、读路径 merge、compaction merge 中的逻辑记录。

#### 说明

`Entry` 是整个项目最基础的数据载体。memtable、SST、迭代器、读合并、压缩归并全部围绕它工作。`Tombstone=true` 表示删除标记。

---

## `internal/wal`

### 目录职责

负责 WAL 的追加、批量追加、同步与重放。它是写路径持久化保证的第一层。

### 文件：`wal.go`

#### 结构体与常量

- `Record`
  - WAL 中的逻辑记录，包含操作类型、key、value。

- `WAL`
  - 封装文件句柄、缓冲写和互斥锁。

- `ErrCorruptWAL`
  - 标识 WAL 内容损坏。

- `opPut` / `opDelete`
  - WAL 层面的记录操作码。

#### 函数

- `Open(path string) (*WAL, error)`
  - 打开或创建 WAL 文件，并初始化缓冲写。

- `(*WAL) Close() error`
  - 刷出缓冲并关闭 WAL 文件。

- `(*WAL) Sync() error`
  - 刷新缓冲并对底层文件做 `fsync`。

- `(*WAL) AppendPut(key string, value []byte) error`
  - 追加一条 put 记录。

- `(*WAL) AppendDelete(key string) error`
  - 追加一条 delete 记录。

- `(*WAL) AppendBatch(records []Record) error`
  - 一次追加多条记录，是 write group commit 的底层 WAL 接口。

- `appendRecord(w *bufio.Writer, rec Record) error`
  - 编码并写出单条 WAL 记录。

- `Replay(path string) ([]Record, error)`
  - 顺序重放 WAL，返回记录列表。允许忽略截断尾记录，用于崩溃恢复。

### 文件：`wal_test.go`

#### 测试

- `TestWALAppendAndReplay`
  - 验证单条 put/delete 记录追加并可正确回放。

- `TestWALAppendBatchAndReplay`
  - 验证批量追加与顺序回放。

- `TestWALReplay_TruncatedTailRecordIgnored`
  - 验证损坏尾部或截断尾部不会破坏前面的有效记录恢复。

### 当前状态

WAL 层是清晰和自洽的：

- 支持单条与批量追加。
- 支持显式同步。
- 支持恢复时忽略尾部半写记录。

---

## `internal/manifest`

### 目录职责

维护当前层级布局的持久化快照。当前模型是 snapshot-style manifest，而不是 VersionEdit log。

### 文件：`manifest.go`

#### 结构体与错误

- `Snapshot`
  - 包含 `NextFile` 和 `Levels`。
  - 表示某一时刻完整层级状态的持久化快照。

- `Manifest`
  - 封装 MANIFEST 文件写入。

- `ErrNoSnapshot`
  - 表示 MANIFEST 中没有有效快照。

#### 函数

- `Open(path string) (*Manifest, error)`
  - 打开或创建 MANIFEST 文件。

- `(*Manifest) Close() error`
  - 关闭 MANIFEST。

- `(*Manifest) WriteSnapshot(next uint64, levels [][]string) error`
  - 追加一条完整快照。

- `(*Manifest) CheckpointLatest() error`
  - 将 MANIFEST 压缩成只保留最新快照，用于 checkpoint。

- `LoadLastSnapshot(path string) (*Snapshot, error)`
  - 从 MANIFEST 中加载最后一个有效快照。

### 文件：`manifest_test.go`

#### 辅助函数

- `countLinesByNewline`
  - 用于测试 checkpoint 后行数变化。

#### 测试

- `TestManifest_WriteAndLoadLastSnapshot`
  - 验证写入与读取最新快照。

- `TestLoadLastSnapshot_TolerateCorruptTrailingLine`
  - 验证尾部损坏行不影响读取最后一个有效快照。

- `TestLoadLastSnapshot_NoSnapshot`
  - 验证空 MANIFEST 的行为。

- `TestManifest_CheckpointLatest_CompactsToOneLineAndCanAppend`
  - 验证 checkpoint 后只剩最新快照，且后续仍可追加。

### 当前状态

Manifest 路径已经可用且闭环成立，但仍是“完整快照式”而非工业级 edit log。

---

## `internal/memtable`

### 目录职责

提供内存有序表实现。当前是 skiplist 驱动的 memtable。

### 文件：`skiplist.go`

#### 结构体

- `node`
  - skiplist 节点，持有 key、entry 和各层 next 指针。

- `SkipList`
  - 封装头节点和层高信息。

#### 函数

- `NewSkipList() *SkipList`
  - 创建新的 skiplist。

- `(*SkipList) randomLevel() int`
  - 生成节点层高。

- `(*SkipList) Search(key string) (types.Entry, bool)`
  - 查找指定 key。

- `(*SkipList) Upsert(key string, entry types.Entry)`
  - 插入或更新指定 key。

- `(*SkipList) First() *node`
  - 返回首节点。

- `(*SkipList) FirstGE(target string) *node`
  - 返回第一个大于等于目标 key 的节点。

### 文件：`memtable.go`

#### 结构体

- `MemTable`
  - 封装 skiplist 与近似字节数统计。

#### 函数

- `NewMemTable() *MemTable`
  - 创建空 memtable。

- `(*MemTable) Put(key string, value []byte)`
  - 写入键值。

- `(*MemTable) Get(key string) ([]byte, bool)`
  - 获取 key 对应的值，不区分 tombstone 细节。

- `(*MemTable) GetAll(key string) (types.Entry, bool)`
  - 获取完整 `Entry`，包括 tombstone。

- `(*MemTable) Delete(key string)`
  - 写入 tombstone。

- `(*MemTable) Range(start, end string) []types.Entry`
  - 范围扫描，只返回最终读语义需要的数据。

- `(*MemTable) RangeAll(start, end string) []types.Entry`
  - 范围扫描，返回完整 `Entry`。

- `cloneBytes(b []byte) []byte`
  - 克隆字节切片，避免外部修改内部状态。

- `(*MemTable) ApproxSize() int`
  - 返回近似大小，用于 auto flush 和背压判断。

### 文件：`memtable_test.go`

#### 测试

- `TestMemTablePutGet`
  - 基本写读正确性。

- `TestMemTablePutOverwrite`
  - 覆盖写行为正确。

- `TestMemTableDelete`
  - tombstone 读语义正确。

- `TestMemTableRange`
  - 范围扫描有序且边界正确。

- `TestMemTableReturnsClonedBytes`
  - 验证返回值已拷贝，不暴露内部切片。

### 当前状态

memtable 层语义清晰，但不是并发安全容器。并发安全由上层 DB 的写串行和快照边界保证。

---

## `internal/sstable`

### 目录职责

实现 SST 文件格式、顺序迭代、点查、索引、bloom 和 table cache。

### 文件：`result.go`

#### 内容

- `GetResult`
  - 点查结果枚举。

- `ErrCorruptSST`
  - SST 损坏错误。

- `NotFound` / `Found` / `Deleted`
  - 点查状态。

### 文件：`footer.go`

#### 函数

- `loadFooter(f *os.File, fileSize int64) (...)`
  - 读取 SST footer，解析索引区、bloom 区和 meta 区偏移。

### 文件：`meta.go`

#### 函数

- `loadBounds(f *os.File, fileSize int64) (string, string, error)`
  - 从文件中解析 min/max key。

- `LoadBounds(path string) (string, string, error)`
  - 面向路径的边界加载接口。

### 文件：`bloom.go`

#### 结构体

- `bloom`
  - 简单 bloom filter。

#### 函数

- `newBloom`
- `(*bloom) add`
- `(*bloom) mayContain`
- `(*bloom) set`
- `(*bloom) get`
- `(*bloom) marshal`
- `unmarshalBloom`
- `fnv64a`
- `mix64`

这些函数共同负责 bloom 构建、查询和序列化。

### 文件：`index.go`

#### 结构体与常量

- `indexEntry`
  - 稀疏索引项。

- `indexStride`
  - 索引步长。

- `maxIndexKeySize`
  - 索引 key 上限。

- `maxIndexCount`
  - 索引项数量上限。

#### 函数

- `loadIndex`
  - 从 SST 加载稀疏索引。

- `pickScanRange`
  - 根据索引为点查或顺序扫描缩小扫描范围。

### 文件：`iter.go`

#### 结构体

- `Iter`
  - 顺序遍历 SST 记录的迭代器。

#### 函数

- `NewIter(path string) (*Iter, error)`
  - 打开 SST 迭代器。

- `(*Iter) Valid() bool`
  - 当前项是否有效。

- `(*Iter) Entry() types.Entry`
  - 返回当前记录。

- `(*Iter) Next() error`
  - 推进到下一条记录。

- `(*Iter) Close() error`
  - 关闭底层文件。

- `(*Iter) readNext() error`
  - 实际读取下一条记录。

### 文件：`sstable.go`

#### 结构体与常量

- `countWriter`
  - 包装带计数功能的 buffered writer。

- `magic`
  - SST 文件 magic。

- `headerSize`
  - 记录头大小。

#### 函数

- `newCountWriter`
- `(*countWriter) Write`
- `(*countWriter) WriteByte`
- `(*countWriter) Flush`
  - 支撑构建 SST 时的字节计数。

- `WriteTable(path string, entries []types.Entry) error`
  - 写出完整 SST，包括数据区、索引、bloom、meta、footer。

- `Get(path string, key string) ([]byte, GetResult, error)`
  - 直接对单个 SST 做点查。

### 文件：`table_cache.go`

#### 结构体

- `TableMeta`
  - 缓存单个 SST 的文件句柄、边界、bloom、索引等元信息。

- `TableCache`
  - SST 元信息缓存。

#### 函数

- `openTableMeta(path string) (*TableMeta, error)`
  - 打开并解析单个 SST 元信息。

- `(*TableMeta) Get(key string) ([]byte, GetResult, error)`
  - 基于缓存元信息做高效点查。

- `(*TableMeta) ensureIndex() error`
  - 惰性加载索引。

- `NewTableCache() *TableCache`
  - 构建缓存。

- `(*TableCache) Get(path string, key string) ([]byte, GetResult, error)`
  - 通过缓存层执行点查。

- `(*TableCache) Evict(path string)`
  - 驱逐某个 SST 的元信息。

- `(*TableCache) Close() error`
  - 关闭所有缓存的 table meta。

- `(*TableCache) Len() int`
  - 缓存项数。

- `(*TableCache) getMeta(path string) (*TableMeta, error)`
  - 获取或加载指定路径的元信息。

### 测试文件与覆盖

- `bloom_test.go`
  - `TestBloomSkipsCorruptIndexOnMiss`
  - 验证 bloom miss 能在索引损坏时提前跳过。

- `index_test.go`
  - `TestIndexLimitsScanRange`
  - `TestIndexCorruptionInsideRange`
  - `TestIndexFooterOutOfRange`
  - 验证稀疏索引边界、损坏处理和 footer 越界处理。

- `iter_test.go`
  - `TestIter_ScanAllRecordsInOrder`
  - `TestIter_EmptyTable`
  - 验证顺序扫描与空表行为。

- `meta_test.go`
  - `TestLoadBounds`
  - 验证边界加载。

- `sstable_test.go`
  - `TestSSTableWriteAndGet`
  - `TestSSTableMagicMismatch`
  - 验证点查与 magic 校验。

- `table_cache_test.go`
  - `TestTableCache_ReusesParsedMetadata`
  - `TestTableCache_EvictReloadsTable`
  - 验证缓存复用与驱逐重载。

### 当前状态

SSTable 层已经比较完整：

- 支持写表、点查、顺序迭代。
- 有稀疏索引与 bloom。
- 有 table meta cache。
- 有损坏与边界条件测试。

---

## `internal/db`

### 目录职责

这是引擎主目录。它负责：

- DB 生命周期管理。
- Version 与 Manifest 协调。
- 写队列与 group commit。
- memtable / immutable queue / flush。
- 读路径快照。
- compaction 调度、执行与安装。
- 文件恢复与清理。

当前设计是“单 writer + immutable queue flush + 单 worker compaction”。

### 文件：`db_state.go`

#### 结构体

- `levelFile`
  - 某层某个 SST run 的描述，包含 `id/path/minKey/maxKey`。

- `level`
  - 某层元信息。
  - L0 主要用 `l0Paths`，L1+ 主要用 `runs`。

- `Version`
  - 当前层级布局只读视图。

- `immutableMem`
  - 冻结后的 memtable。
  - 当前包含 memtable 指针、代际号、对应 flush WAL 路径等队列信息。

- `pointReadView`
  - 点查快照。
  - 为 `Get()` 收集 active mem 命中结果、immutable queue 和 version。

- `scanReadView`
  - 范围读快照。
  - 为 `Scan()` 收集 active mem 范围结果、immutable queue 和 version。

- `writeState`
  - 写路径运行态。
  - 包括写队列、关闭标记、当前 WAL sync 函数、sync loop 通道、seq 边界等。

- `flushState`
  - flush 路径运行态。
  - 包括条件变量、auto flush 通道、flush request、running 状态、当前 job、代际边界等。

- `compactionState`
  - compaction 路径运行态。
  - 包括条件变量、请求标记、pendingPaths。

- `DB`
  - 引擎主对象。
  - 聚合目录路径、options、WAL、memtable、immutable queue、current version、manifest、table cache 和各类运行态。

#### 函数

- `(*DB) currentVersion() *Version`
  - 返回当前 version，内部处理 nil 接收者等边界。

### 文件：`db_open_close.go`

#### 函数

- `Open(dir string) (*DB, error)`
  - 使用默认配置打开 DB。

- `OpenWithOptions(dir string, opt Options) (*DB, error)`
  - 核心打开流程。
  - 主要负责：
    - 校验配置
    - 创建目录
    - 打开 WAL / MANIFEST / table cache
    - 恢复 version
    - 清理 orphan / tmp 文件
    - 回放 flush WAL 与 active WAL
    - 启动后台 writer / wal sync / flush / auto flush / compaction loop

- `listFlushWALPaths(dir string) ([]string, error)`
  - 扫描 `forge.flush.<gen>.wal`，按顺序返回，用于恢复。

- `(*DB) Close() error`
  - 停止后台协程，等待 in-flight 工作结束，关闭 WAL、MANIFEST 和 cache。

- `(*DB) persistManifestLevels(levels []level) error`
  - 将新的层级快照持久化到 MANIFEST，并按阈值做 checkpoint。

### 文件：`db_options.go`

#### 结构体与类型

- `LevelOptions`
  - 每层配置，如 `maxFiles`、`pickN`、`runMaxEntries` 等。

- `WALSyncMode`
  - WAL 同步策略枚举：
    - `WALSyncNever`
    - `WALSyncEveryBatch`
    - `WALSyncInterval`

- `Options`
  - DB 整体配置，包括层级布局、WAL sync、auto flush 阈值、immutable queue 上限等。

#### 函数

- `DefaultOptions() Options`
  - 对外默认配置。

- `(Options) WithWALSync(mode WALSyncMode, interval time.Duration) Options`
  - 链式配置 WAL 同步模式。

- `(Options) WithAutoFlushBytes(bytes int) Options`
  - 配置 auto flush 阈值。

- `(Options) WithMaxImmutableMems(max int) Options`
  - 配置 immutable queue 上限，控制 flush 背压。

- `defaultOptions() Options`
  - 内部默认配置。

- `validateOptions(opt Options) error`
  - 校验配置合法性。

### 文件：`db_write.go`

#### 类型

- `writeOp`
  - 内部写操作枚举，put/delete。

- `batchOp`
  - 批量写中的单条操作。

- `writeReq`
  - 写队列中的请求单元。

- `WriteBatch`
  - 公开批量写容器。

- `errDBClosed`
  - DB 已关闭错误。

#### 函数

- `(*WriteBatch) Put`
  - 向批量写中追加 put。

- `(*WriteBatch) Delete`
  - 向批量写中追加 delete。

- `(*WriteBatch) Len`
  - 返回批次操作数。

- `(*DB) Put`
  - 公开单条 put。

- `(*DB) Delete`
  - 公开单条 delete。

- `(*DB) Write`
  - 提交一批 `WriteBatch`。

- `(*DB) submitWrite`
  - 将单条写包装成 batch 并提交到写队列。

- `(*DB) submitWriteBatch`
  - 将一批操作提交到写队列。

- `(*DB) writeLoop`
  - writer 后台主循环，负责 group commit。

- `(*DB) collectWriteBatch`
  - 在短时间窗口内聚合多个写请求。

- `(*DB) applyWriteBatch`
  - 单批 group commit 主流程。
  - 当前分成三段：
    1. `appendWriteBatchWALLocked`
    2. `applyBatchToMemtable`
    3. `waitWriteFlushBackpressureLocked`

- `(*DB) appendWriteBatchWALLocked`
  - 将一批写转换为 WAL 记录并写入 WAL，同时处理 WAL sync 策略和 seq 边界。

- `(*DB) waitWriteFlushBackpressureLocked`
  - 当 flush queue 满时等待队列腾出空间，实现写路径背压。

- `(*DB) walSyncLoop`
  - interval sync 模式下的后台 WAL 定时同步 loop。

- `(*DB) syncPendingWAL`
  - 执行一次 WAL sync，并推进 `syncedSeq`。

- `cloneBatchOps`
  - 克隆 `batchOp` 切片。

- `buildWALRecords`
  - 将写请求批次转换为 WAL 记录数组。

- `applyBatchToMemtable`
  - 将批次真正应用到 active memtable。

- `cloneWriteValue`
  - 克隆写入值，避免引用调用方切片。

### 文件：`db_read.go`

#### 函数与类型

- `(*DB) Get(key string) ([]byte, bool, error)`
  - 点查入口。
  - 先抓 `pointReadView`，锁外再读 immutable queue 和 SST。

- `(*DB) Scan(start, end string) ([]types.Entry, error)`
  - 范围读入口。
  - 先抓 `scanReadView`，再锁外扫 immutable queue 和 SST 并 merge。

- `(*DB) NewIterator(start, end string) (*Iterator, error)`
  - 基于 `Scan()` 构造简单迭代器。

- `Iterator`
  - 基于静态结果集的只读迭代器。

- `(*Iterator) Valid`
- `(*Iterator) Entry`
- `(*Iterator) Next`
- `(*Iterator) Close`
  - 提供简单顺序访问能力。

- `(*DB) capturePointReadView`
  - 为点查捕获快照。
  - 由于 active memtable 仍可写，所以会在锁内先物化 active mem 命中结果。

- `(*DB) captureScanSnapshot`
  - 为范围读捕获快照。

- `scanSSTRange`
  - 顺序扫描某个 SST 范围。

- `runOverlapsRange`
  - 判断某个 run 是否与给定范围重叠。

- `mergeScanSources`
  - 将多来源范围结果做去重和优先级合并。

- `cloneReadBytes`
  - 返回值切片拷贝，避免向外暴露内部数据。

### 文件：`db_flush.go`

#### 类型

- `flushJob`
  - 单个 flush 任务执行上下文。
  - 当前主要绑定队头 `immutableMem` 及目标 SST 路径。

#### 函数

- `(*DB) Flush() error`
  - 公开显式 flush。
  - 负责触发 active mem 冻结入队并等待至少一轮 flush 完成。

- `(*DB) autoFlushLoop()`
  - 后台 auto flush 监听器。
  - 响应 active memtable 达到阈值后的请求。

- `(*DB) flushLoop()`
  - 后台 flush worker。
  - 只消费队头 immutable，执行 `doFlush + installFlushLocked`。

- `(*DB) doFlush(job *flushJob) error`
  - 将一个 immutable memtable 写成 SST 临时文件并原子安装目标文件。

- `(*DB) installFlushLocked() error`
  - 在持锁状态下安装 flush 结果：
    - 更新 version
    - 持久化 manifest
    - 出队队头 immutable
    - 清理对应 flush WAL

- `(*DB) requestAutoFlush()`
  - 向 auto flush loop 发送“检查阈值”的信号。

- `(*DB) requestFlushLocked()`
  - 向 flush worker 标记有工作并唤醒。

- `(*DB) shouldAutoFlushLocked() bool`
  - 判断 active mem 是否达到 auto flush 条件。

- `(*DB) flushQueueFullLocked() bool`
  - 判断 immutable queue 是否达到上限。

- `(*DB) hasFlushWorkLocked() bool`
  - 判断系统当前是否仍有 flush 相关工作。

- `(*DB) waitFlushQueueRoomLocked() error`
  - 等待 immutable queue 腾出空位，实现背压。

- `(*DB) waitFlushDoneLocked(target uint64) error`
  - 等待指定代际之前的 flush 完成。

- `(*DB) enqueueActiveMemtableLocked() (*immutableMem, error)`
  - 将 active memtable 冻结成 immutable 并入队，同时轮转 WAL。

- `(*DB) startFlushJobLocked() *flushJob`
  - 以队头 immutable 构造一轮可执行的 flush job，并预留 SST 文件号。

- `(*DB) freezeActiveMemLocked() *memtable.MemTable`
  - 从当前 active mem 切换到新的 active mem。

- `(*DB) rotateFlushWALLocked(imm *memtable.MemTable, flushWALPath string) error`
  - 将当前 `forge.wal` 轮转为某份 immutable 对应的 `forge.flush.<gen>.wal`，并打开新的活跃 WAL。

- `flushWALPath(dir string, gen uint64) string`
  - 生成新格式 flush WAL 路径。

### 文件：`db_storage.go`

#### Version 与恢复

- `newVersionFromLevels`
  - 根据层级布局构造新 version。

- `(*Version) withLevels`
  - 克隆当前 version 并返回可变副本。

- `cloneLevels`
  - 深拷贝层级。

- `recoverFromManifest`
  - 从 MANIFEST 恢复层级布局和 `nextID`。

- `levelsToSnapshot`
  - 将层级转成 MANIFEST 需要的快照结构。

- `scanLevel0`
  - 从 L0 目录扫描 SST。

- `scanLevelN`
  - 从 L1+ 目录扫描并恢复 run 列表。

- `scanAllLevels`
  - 在没有 MANIFEST 快照时，通过目录扫描恢复整套层级。

#### 文件清理与目录健壮性

- `cleanupTmpFiles`
  - 清理 `.tmp` 临时文件。

- `sanityCheckSSTDir`
  - 检查目录中的 orphan SST。

- `syncDirFn`
  - 目录同步函数变量，便于测试替换。

- `syncDirIfSupported`
  - 在平台支持时做目录 `fsync`。

- `syncPathDir`
  - 同步某路径所在目录。

- `removeFileAndSync`
  - 删除文件并同步所在目录。

- `isDirSyncUnsupported`
  - 判断目录同步是否属于平台不支持。

- `isTransientRemoveErr`
  - 判断删除失败是否属于可容忍的瞬时错误。

- `listAllSSTFiles`
  - 列出所有 SST 文件。

- `referencedSSTSet`
  - 构造当前 version 引用的 SST 集合。

- `findOrphanSST`
  - 找出未被 version 引用的孤儿 SST。

#### 工具函数

- `removePaths`
  - 从路径列表移除指定路径。

- `removeRunsByPaths`
  - 从 runs 列表移除指定路径。

- `parseSSTID`
  - 从 SST 文件名提取 ID。

- `sstKeyRange`
  - 返回某个 SST 的 key 范围。

- `rangeOfFiles`
  - 计算一组 SST 文件覆盖的整体 key 范围。

- `pickOldestL0`
  - 从 L0 按旧到新选择输入。

- `pickOldestRuns`
  - 从 L1+ run 列表中选择输入。

- `findRun`
  - 在不重叠 run 列表中定位某个 key 可能落入的 run。

- `overlappedRuns`
  - 找出与某个范围重叠的 runs 下标。

- `(*DB) writeSingleRun`
  - 将一批 entry 写成单个目标层 run。

### 文件：`db_compaction.go`

#### 函数

- `(*DB) RequestCompaction() error`
  - 对外触发一次 compaction 请求。

- `(*DB) compactionLoop()`
  - 后台 compaction worker 主循环。

- `(*DB) runCompactionStep() bool`
  - 单 worker 执行一轮完整 compaction：
    - 选层
    - pick job
    - do compaction
    - install

- `(*DB) requestCompactionLocked()`
  - 标记有 compaction 请求并唤醒 worker。

- `(*DB) checkBGErrLocked() error`
  - 返回当前后台错误。

### 文件：`db_compaction_picker.go`

#### 结构体

- `compactionJob`
  - 单个压缩任务对象。
  - 当前保留最小必要字段：
    - `level`
    - `srcPaths`
    - `dstPaths`
    - `minKey`
    - `maxKey`

#### 函数

- `(*compactionJob) dstLevel() int`
  - 返回目标层号。

- `(*compactionJob) allPaths() []string`
  - 返回本轮任务占用的所有路径，用于 pending 标记与释放。

- `(*DB) pickCompactionLevelLocked() (int, bool)`
  - 从所有层中选择最值得压缩的层。

- `(*DB) pickCompactionJobLocked(level int) (*compactionJob, error)`
  - 构造某层的一轮 compaction job。
  - 内部包含：
    - 选源输入
    - 算源范围
    - 找目标层 overlap
    - 标记 pending

- `(*DB) pickCompactionInputsLocked`
  - 选择源层输入。

- `(*DB) pickCompactionOverlapLocked`
  - 选择目标层重叠输入，并检查 pending 冲突。

- `(*DB) isPendingLocked`
  - 检查某个路径是否已 pending。

- `(*DB) markPendingLocked`
  - 占用一批路径。

- `(*DB) clearPendingLocked`
  - 释放一批路径。

### 文件：`db_compaction_run.go`

#### 结构体

- `heapItem`
  - 多路归并堆中的单路输入项。

- `mergeHeap`
  - 归并堆。

#### 函数

- `Len`
- `Less`
- `Swap`
- `Push`
- `Pop`
  - 满足 heap.Interface。

- `(*DB) doCompaction(job *compactionJob) ([]levelFile, error)`
  - 根据 job 执行压缩，生成新的 runs。

- `(*DB) streamCompactionRuns(dstLevel int, inputs []string, dropBottomTombstones bool) ([]levelFile, error)`
  - 流式合并多个 SST 输入。
  - 处理去重、优先级、底层墓碑丢弃、run 分段写出、失败清理。

### 文件：`db_compaction_install.go`

#### 函数

- `(*DB) installCompactionLocked(job *compactionJob, newRuns []levelFile) error`
  - 安装 compaction 结果：
    - 从源层移除输入
    - 在目标层重新计算真实 overlap
    - 替换目标层区间
    - 持久化 MANIFEST
    - 切换 current version
    - 删除旧输入与被替换文件
    - 释放 pending

- `compactionInstallOverlap(runs []levelFile, minK, maxK string) ([]int, []string)`
  - 安装阶段重新计算真实 overlap 区间与旧路径列表。

- `replaceCompactionDstRuns(oldRuns []levelFile, overIdx []int, newRuns []levelFile) []levelFile`
  - 在目标层替换重叠区间。

- `(*DB) failCompactionLocked(job *compactionJob, err error)`
  - compaction 失败收口：
    - 释放 pending
    - 设置后台错误

### `internal/db` 测试文件与覆盖

#### `db_state_test.go`

- `TestNewVersionFromLevels_ClonesInput`
  - 验证从层级构造 Version 时会深拷贝输入。

- `TestVersion_WithLevels_DoesNotMutateOldVersion`
  - 验证 `withLevels()` 返回的新版本不会污染旧版本。

- `TestVersion_WithLevels_NilReceiver`
  - 验证 nil version 接收者的边界行为。

#### `db_options_test.go`

- `TestDefaultOptions_UsesNeverSyncByDefault`
  - 验证默认 WAL sync 和默认 immutable queue 上限。

- `TestOptionsWithWALSync_ConfiguresIntervalMode`
  - 验证 interval sync 配置。

- `TestOptionsWithWALSync_KeepsExistingIntervalForNonIntervalMode`
  - 验证切换 WAL sync 模式时的 interval 保留行为。

- `TestOptionsWithAutoFlushBytes_ConfiguresThreshold`
  - 验证 auto flush 阈值配置。

- `TestOptionsWithMaxImmutableMems_ConfiguresQueueLimit`
  - 验证 immutable queue 上限配置。

#### `db_write_sync_test.go`

- `TestDBWALSyncNever_DoesNotCallSync`
  - 验证 never 模式不调用 fsync。

- `TestDBWALSyncEveryBatch_CallsSync`
  - 验证 every-batch 模式每批次 fsync。

- `TestDBWALSyncInterval_CallsSyncAfterInterval`
  - 验证 interval 模式后台定时触发 fsync。

- `TestDBWALSyncError_DoesNotApplyMemtable`
  - 验证 fsync 失败时该批写不会应用进 memtable。

- `TestDBWALSyncInterval_ErrorEventuallySetsBGErr`
  - 验证 interval 模式异步 sync 失败最终会暴露为后台错误。

- `waitForCondition`
  - 测试用轮询等待辅助。

#### `db_read_test.go`

- `TestDBScan_MergesMemImmAndSST`
  - 验证范围读能正确合并 mem、immutable 和 SST。

- `TestDBIterator_RespectsRangeBounds`
  - 验证迭代器范围边界正确。

- `TestDBScan_AfterCompactionIntoL1`
  - 验证 compaction 后范围读在更深层上仍正确。

#### `db_flush_auto_test.go`

- `TestDBAutoFlush_TriggersWithoutExplicitFlush`
  - 验证 auto flush 可在无显式 `Flush()` 下触发。

- `TestDBAutoFlush_ForegroundWritesRemainVisible`
  - 验证 auto flush 期间前台写读语义保持正确。

#### `db_flush_test.go`

##### 辅助函数

- `snapshotFlushStateLocked`
  - 抓取 flush 状态快照。

- `fileExists`
  - 检查文件是否存在。

- `prepareFlushJobForTest`
  - 测试内手动构造一轮队头 flush job。

##### 测试

- `TestDB_FlushPrepare_RotatesMemAndWAL`
  - 验证冻结 mem 和轮转 WAL。

- `TestDB_Flush_ForegroundWritesDuringFlushAndInstallCleansUp`
  - 验证 flush 期间前台写仍可见，安装后文件清理正确。

- `TestDB_Reopen_RecoversFlushWALAndActiveWAL`
  - 验证重启恢复 flush WAL 和 active WAL。

- `TestDB_Reopen_RecoversWhenFlushSSTExistsButInstallNotDone`
  - 验证 flush 输出已落盘但未 install 的恢复。

- `TestDB_MultipleFlushes_NoImmLeakAndReadsCorrect`
  - 验证多轮 flush 后 immutable queue 不泄漏且读取正确。

- `TestDB_FlushQueue_SecondImmutableBecomesNewHead`
  - 验证队头 install 后下一份 immutable 成为新的队头。

- `TestDB_FlushQueue_BackpressureWaitsForRoom`
  - 验证 immutable queue 背压会阻塞入队直到有空位。

- `TestDB_Flush_FileLayout_NoTmpLeft`
  - 验证 flush 后没有遗留临时文件。

- `TestDB_Flush_AdoptsPreparedJob`
  - 验证 flushLoop 能接住已准备好的 job。

#### `db_compaction_test.go`

##### 辅助函数

- `assertRunsSortedNoOverlap`
  - 检查 L1+ runs 的排序和不重叠不变式。

- `waitUntil`
  - 测试中等待后台最终一致性条件成立。

- `k`
- `v`
- `keyA`
  - 生成测试 key/value。

- `snapshotLevels`
  - 抓取 L0/L1/L2 文件数量快照。

- `assertInvariantsLocked`
  - 检查层级不变式。

- `mustBuildManualL0Compaction`
  - 测试用手工构造一轮 L0 compaction。

- `installCompactionVersionWithoutCleanup`
  - 测试用安装 compaction 版本但保留旧文件，用于模拟崩溃。

##### 测试

- `TestDB_PickCompactionLevel_PrefersMostOverloadedLevel`
  - 验证调度优先最超载层。

- `TestDB_PickCompactionJob_SkipsWhenDstOverlapPending`
  - 验证目标层 overlap pending 时放弃任务。

- `TestDB_PickCompactionJob_L0InputOrderStable`
  - 验证 L0 输入选择顺序稳定。

- `TestDB_PickCompactionJob_SkipsWhenAllSourceInputsPending`
  - 验证源层输入全部 pending 时放弃任务。

- `TestDB_RunCompactionStep_NoWorkReturnsFalse`
  - 验证没活时单步 compaction 返回 false。

- `TestDB_RunCompactionStep_CompletesAndClearsPending`
  - 验证一轮 compaction 能完成并释放 pending。

- `TestDB_InstallCompactionFailure_ClearsPendingAndSetsBGErr`
  - 验证 install 失败时会清 pending 并设置后台错误。

- `TestDB_TombstoneAndReopen_Correctness`
  - 验证 tombstone 经 compaction 和 reopen 后仍正确。

- `TestDB_KeepsNewestValue`
  - 验证同 key 多版本保留最新值。

- `TestDB_Compaction_KeepsNewestValueAcrossL0`
  - 验证跨多个 L0 文件压缩仍保留最新值。

- `TestDB_Compaction_DropsBottomLevelTombstones`
  - 验证底层压缩会丢弃 tombstone。

- `TestDB_BackgroundCompaction_EventuallyL0ToL1`
  - 验证后台 compaction 最终推进 L0 -> L1。

- `TestDB_BackgroundCompaction_EventuallyThreeLevelsToL2`
  - 验证后台 compaction 最终推进到更深层。

- `TestDB_BackgroundCompaction_ForegroundOpsRemainCorrect`
  - 验证后台压缩期间前台读写仍正确。

- `TestDB_Reopen_RemovesOrphanCompactionOutputsBeforeInstall`
  - 验证崩溃在 install 之前时，会清理 orphan compaction outputs。

- `TestDB_Reopen_CleansOldCompactionInputsAfterManifestInstalled`
  - 验证 install 已写 MANIFEST 但旧文件未删时，重启会清理旧输入。

### 当前状态

`internal/db` 当前已具备以下自洽性：

- Version + MANIFEST 基本闭环。
- writer queue + group commit 已成立。
- active memtable / immutable queue / flush 单 worker 已自洽。
- 读路径已开始快照化。
- obsolete file 清理和恢复路径都有测试。
- 单 worker compaction 的调度规则、安装规则和失败收口已较完整。

但当前仍不是工业级最终态：

- Manifest 仍是 snapshot 式。
- compaction 仍是单 worker。
- 读路径 `Scan/Iterator` 仍偏物化，不是流式迭代器。
- benchmark 体系还未建立。

---

## `internal/dbtest`

### 目录职责

放黑盒集成测试，只通过公开 API 验证端到端行为，不直接访问 `internal/db` 未导出细节。

### 文件：`integration_test.go`

#### 测试

- `TestDBPutGet`
  - 验证最基本的 put/get。

- `TestDBPutFlushReopen`
  - 验证显式 flush 之后重启仍可读取。

- `TestDBDeleteTombstone`
  - 验证 tombstone 落盘后不会被旧 SST 复活。

- `TestDBDeleteOverridesSST`
  - 验证 mem 中 tombstone 能覆盖旧 SST 值。

- `TestDBConcurrentPuts`
  - 验证并发 Put 之后所有键值都正确可读。

- `TestDBWriteBatch_AppliesAllOperations`
  - 验证批量写会统一应用所有操作。

- `TestDBWriteBatch_FlushAndReopen`
  - 验证批量写经过 flush 和重启后仍正确。

#### 辅助函数

- `makeTestKey`
  - 生成并发测试 key。

- `makeTestValue`
  - 生成并发测试 value。

### 当前状态

`internal/dbtest` 目前主要覆盖公开 API 的端到端行为，和 `internal/db` 白盒测试形成互补：

- `internal/db` 偏白盒、规则、状态机测试。
- `internal/dbtest` 偏黑盒、公开 API 和恢复结果测试。

---

## 当前项目的整体评价

只从“当前项目内部是否完整自洽、是否有明显致命问题”来看：

- 当前实现已经高于一般练手型原型。
- 没有明显的致命架构问题。
- 主要关键路径都有测试覆盖：
  - WAL 恢复
  - mem/immutables/flush
  - compaction 调度与恢复
  - tombstone 语义
  - reopen 正确性
  - 并发 Put
  - WAL sync 策略

当前更像：

- 一个高质量、结构清楚的单机 LSM 引擎雏形
- 而不是工业级最终产品

最明显还欠缺的方向是：

1. benchmark 体系。
2. 更系统的故障注入矩阵。
3. 更正式的 VersionSet / VersionEdit 模型。
4. 流式 scan/iterator。
5. 并行 compaction 设计与实现。

