# MonolithDB

MonolithDB 是一个单机、嵌入式、LSM 风格的 KV 存储引擎实验项目。  
当前实现已经覆盖了写前日志、memtable、immutable flush、SSTable、后台 compaction、Version + Manifest 恢复闭环，以及一组较完整的白盒/黑盒测试。

它目前更接近“高质量工程化原型”而不是工业级成品，但核心路径已经基本自洽。

## 项目目标

这个项目关注的是一条清晰、可验证的单机存储引擎主线：

1. 写入统一进入 writer queue。
2. `writeLoop` 做 group commit。
3. 先写 WAL，再写 active memtable。
4. active memtable 冻结为 immutable memtable，并进入 flush 队列。
5. 后台 `flushLoop` 将 immutable memtable 刷成 SST。
6. 后台 `compactionLoop` 执行层级压缩。
7. 读路径按 `mem -> imms -> SST` 查找。
8. `Version + Manifest` 负责版本切换与重启恢复。

## 当前能力

### 写路径

- 单 writer queue
- group commit
- `WALSyncNever / WALSyncEveryBatch / WALSyncInterval`
- `WriteBatch` 批量写接口
- 写路径会在 flush 队列满时触发背压

### 内存与落盘

- active memtable
- immutable memtable 队列
- 后台单 worker flush
- 每份 immutable memtable 对应独立 `forge.flush.<gen>.wal`
- 支持显式 `Flush()` 和阈值驱动的 auto flush

### 读路径

- `Get`
- `Scan`
- 简单 `Iterator`
- 读路径已做快照化起步
- `Get` 与 `Scan` 返回值会拷贝，不暴露内部切片

### SSTable

- SST 写出
- 点查
- 顺序迭代
- 稀疏索引
- bloom filter
- table cache

### 压缩

- 单 worker compaction
- L0 -> L1 -> L2 多层推进
- tombstone 底层丢弃
- `pendingPaths` 防止重复挑选同一批文件
- pick / run / install 已拆分，语义相对清楚

### 恢复与清理

- WAL 重放恢复
- flush WAL 恢复
- Manifest 快照恢复
- 无 Manifest 时目录扫描恢复
- orphan SST 清理
- `.tmp` 临时文件清理

## 当前并发模型

当前项目不是全路径并行，而是刻意做成几条清晰边界：

- 写入：单 writer，后台 `writeLoop` 串行提交
- flush：immutable queue + 单 worker
- compaction：单 worker
- 读取：快照化后尽量锁外读 immutable/SST

也就是说，它现在强调的是：

- 正确性优先
- 恢复闭环优先
- 状态机可解释优先

而不是一开始就追极限并行度。

## 目录结构

### `internal/types`

基础逻辑记录类型，目前主要是 `types.Entry`。

### `internal/wal`

WAL 追加、批量追加、同步与重放。

### `internal/manifest`

Manifest 快照持久化与加载。  
当前是 snapshot-style manifest，不是 VersionEdit 日志模型。

### `internal/memtable`

skiplist 驱动的 memtable，实现 put/get/delete/range。

### `internal/sstable`

SST 文件格式、顺序迭代、点查、meta、index、bloom、table cache。

### `internal/db`

引擎主目录，按职责拆成：

- `db_state.go`
  - 核心状态定义
- `db_open_close.go`
  - 打开、关闭、Manifest 持久化
- `db_options.go`
  - 配置项和默认配置
- `db_write.go`
  - writer queue、group commit、WAL sync
- `db_read.go`
  - `Get / Scan / Iterator`
- `db_flush.go`
  - immutable queue、flush worker、auto flush、背压
- `db_storage.go`
  - version 构造、恢复、目录扫描、文件工具
- `db_compaction.go`
  - compaction 入口与主循环
- `db_compaction_picker.go`
  - compaction job 挑选与 pending 管理
- `db_compaction_run.go`
  - compaction 执行与多路归并
- `db_compaction_install.go`
  - compaction 安装、旧文件清理、失败收口

### `internal/dbtest`

黑盒集成测试，只通过公开 API 验证端到端行为。

### `docs`

目前包含：

- [docs/internal-architecture.md](docs/internal-architecture.md)
  - 按 `internal/*` 目录逐个总结实现与测试覆盖

## 主要设计点

### 1. Version + Manifest

当前版本管理是“完整快照式”：

- flush / compaction 安装时先持久化 Manifest
- 成功后再切换内存中的 `current Version`
- 打开数据库时优先从 Manifest 恢复
- Manifest 不可用时可退化到目录扫描恢复

这保证了当前模型下的基本恢复闭环。

### 2. writer queue + group commit

前台 `Put/Delete/Write(batch)` 不直接落盘，而是进入写队列。  
后台 `writeLoop` 会在一个很短的时间窗口内聚合请求，统一：

1. 追加 WAL
2. 按 sync 策略决定是否 fsync
3. 应用到 active memtable
4. 处理 flush 背压

### 3. immutable queue + flush

当前 flush 模型不是单个 `imm` 槽位，而是 immutable queue：

- active memtable 满足条件后冻结成 immutable
- immutable 入队
- 后台 `flushLoop` 只消费队头
- 队列达到上限后，写路径会被背压阻塞

### 4. 单 worker compaction

当前 compaction 暂时不做多 worker，而是把单 worker 做清楚：

- 选择最值得压缩的层
- 构造 `compactionJob`
- 多路归并生成新 runs
- 安装版本
- 删除旧文件
- 清 pending
- 失败统一收口到 `bgErr`

这样后续如果要上并行 compaction，地基已经比较清楚。

## 测试覆盖

### `internal/wal`

- WAL 追加与重放
- 批量追加
- 截断尾记录恢复

### `internal/manifest`

- 快照写入与加载
- 尾部损坏容忍
- checkpoint 后继续追加

### `internal/memtable`

- put/get/overwrite/delete
- range
- 返回值拷贝

### `internal/sstable`

- SST 写表与点查
- magic 校验
- iter 顺序扫描
- meta 边界读取
- index 范围裁剪与损坏处理
- table cache 复用与驱逐

### `internal/db`

- Version 深拷贝语义
- WAL sync 三种模式
- `Get / Scan / Iterator`
- auto flush
- flush 队列与背压
- flush/reopen/crash 恢复
- compaction 规则测试
- compaction 后数据正确性
- compaction orphan output 清理
- compaction install 前后崩溃恢复

### `internal/dbtest`

- Put/Get
- Flush/Reopen
- Tombstone 覆盖旧值
- 并发 Put
- WriteBatch 端到端行为

## 当前状态评价

如果只看“完整自洽”和“是否有明显致命问题”，当前项目可以这样评价：

- 已经过了简单练手原型阶段
- 主要路径基本自洽
- 恢复主线基本闭环
- 没有明显致命架构问题

但它仍然不是工业级最终产品，当前仍欠缺：

- benchmark 体系
- 更系统的故障注入矩阵
- 更正式的 VersionSet / VersionEdit 模型
- 流式 scan / iterator
- 并行 compaction

## 后续方向

如果继续往前推进，最自然的路线是：

1. benchmark 与性能基线
2. 更系统的 flush + compaction 交错恢复测试
3. 更正式的 Version / Manifest 模型
4. 流式读路径
5. 并行 compaction

## 参考文档

- [docs/internal-architecture.md](docs/internal-architecture.md)

