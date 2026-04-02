# ForgeDB

## 项目名称

ForgeDB

## 一、项目简介

ForgeDB 是一个使用 Go 自研的单机嵌入式 KV 存储引擎项目，整体采用 LSM-Tree 风格架构。项目围绕存储引擎最核心的几条主线展开：写前日志保证持久化、MemTable 承接前台写入、SSTable 提供有序持久化存储、后台 Flush 与 Compaction 负责内存数据落盘和多层数据整理，最终形成一套较完整的单机存储引擎闭环。

这个项目的目标并不是简单实现一个“能用的键值数据库”，而是尽可能按照真实存储引擎的思路，把写路径、读路径、后台任务、崩溃恢复和测试体系逐步做清楚。当前版本已经具备较完整的工程化原型特征，能够支持基础读写、批量写、后台刷盘、层级压缩、重启恢复以及一定程度的并发访问。

## 二、项目目标

ForgeDB 主要关注以下几个方面：

1. 构建一条清晰、自洽、可恢复的单机存储引擎主线。
2. 用工程化方式实现 LSM-Tree 核心组件，而不是只做概念演示。
3. 在保证正确性的前提下，逐步引入更合理的并发控制和后台调度机制。
4. 通过白盒测试和黑盒测试验证数据正确性、恢复逻辑和后台状态机行为。
5. 为后续继续扩展为更完整的存储内核打下基础。

## 三、整体架构

ForgeDB 当前采用典型的 LSM 风格分层设计，其核心数据流如下：

前台写入首先进入 writer queue，由后台 `writeLoop` 统一进行 group commit。写入先追加到 WAL，再应用到当前 active MemTable。当 active MemTable 达到阈值后，会被冻结为 immutable MemTable，并进入 flush 队列。后台 flush worker 将 immutable MemTable 刷写成新的 SSTable 文件。随着 L0 文件持续累积，后台 compaction worker 会选择合适的层级和文件集合执行压缩，将数据逐步推进到更深层级，并清理冗余版本与可丢弃的墓碑记录。

读取路径则按照“内存优先、磁盘兜底”的思路工作：先查 active MemTable，再查 immutable 队列，最后再查 SSTable。SSTable 层使用稀疏索引、Bloom Filter 和表元信息缓存来减少无效 IO，提高点查效率。

为了在崩溃后恢复系统状态，ForgeDB 引入了 WAL、flush WAL、Manifest 快照以及目录扫描恢复机制，保证数据库在重启时能够尽可能恢复到一致状态。

## 四、核心功能

### 1. 基础 KV 接口

ForgeDB 提供 `Put`、`Get`、`Delete`、`WriteBatch`、`Scan` 和 `Iterator` 等基础接口，支持单键读写、删除、批量写入以及范围扫描。

### 2. WAL 持久化

所有前台写入都会先进入 WAL，再应用到内存结构中，保证崩溃恢复时能够通过日志重放找回尚未落盘的数据。

### 3. Group Commit 写入路径

前台写请求不会直接各自落盘，而是统一进入 writer queue，由后台 `writeLoop` 聚合成批次统一写 WAL 并应用到 MemTable，从而减少频繁刷盘带来的额外开销。

### 4. 可配置 WAL 同步策略

系统当前支持多种 WAL 同步模式，包括：

- `WALSyncNever`：不主动对每批写入执行 `fsync`
- `WALSyncEveryBatch`：每个批次写入后立即 `fsync`
- `WALSyncInterval`：由后台周期性执行 `fsync`

这使得系统可以在吞吐与持久化开销之间做权衡。

### 5. MemTable 与 Immutable Queue

系统使用 SkipList 作为 MemTable 的核心有序结构。当前 active MemTable 达到条件后会被冻结，并进入 immutable 队列。后台 flush worker 只消费队头 immutable，形成更清晰的刷盘调度模型。为了避免后台落盘过慢导致内存无限增长，写路径还引入了 flush 背压机制。

### 6. SSTable 持久化存储

ForgeDB 已实现 SSTable 的写出、点查和顺序迭代，并在文件层支持：

- 稀疏索引
- Bloom Filter
- 边界元信息
- Table Cache

这些机制共同支撑了较高效的磁盘查询路径。

### 7. 多层 Compaction

系统已支持多层 SSTable 的后台压缩，能够将 L0 文件逐步向更深层级推进。压缩过程会处理同 key 多版本去重、tombstone 传播与底层丢弃等语义，并借助 `pendingPaths` 防止重复挑选同一批文件。

### 8. 崩溃恢复与文件清理

数据库重启时会优先从 Manifest 恢复层级布局，在必要时也可通过目录扫描重建版本信息。同时系统支持恢复 active WAL、flush WAL，并清理 orphan SST、遗留临时文件等异常状态文件，使恢复链路形成闭环。

## 五、技术实现要点

### 1. LSM-Tree 存储模型

项目整体采用内存写入、顺序落盘、后台归并的设计思路。相比直接原地更新磁盘数据，LSM 更适合高写入吞吐场景，同时也更便于通过 WAL 和后台任务组织系统状态。

### 2. SkipList MemTable

内存表使用 SkipList 维护键有序性，既能支持高效点查，也便于后续范围扫描和顺序刷盘。

### 3. Writer Queue 与写串行化

为了简化并发写入下的状态一致性问题，当前版本采用单 writer queue 模型，将前台写请求统一交给后台 `writeLoop` 处理。这不是简单的“并发不足”，而是一种以正确性和状态清晰度为优先的工程取舍。

### 4. Immutable Queue 与 Flush 背压

与早期单 immutable 槽位模型不同，当前实现已经扩展为 immutable 队列。这样可以更清楚地组织前台写入、后台刷盘和内存占用之间的关系，并在队列满时阻塞前台写入，避免系统失控。

### 5. Version + Manifest 恢复模型

系统当前采用快照式 Manifest 记录全局层级布局。在 Flush 或 Compaction 安装新版本时，会先持久化 Manifest，再切换当前内存中的 Version，从而保证恢复时能够找到一致的文件布局。

### 6. Pick / Run / Install 分离的 Compaction 设计

Compaction 逻辑被拆分为挑选任务、执行合并、安装新版本三个阶段，使后台压缩的职责边界更清晰，也便于后续继续扩展为更复杂的调度与并行模型。

## 六、当前并发模型

ForgeDB 目前更强调“正确性优先、恢复优先、状态边界清晰”，而不是一开始就追求极限并行度。当前并发模型可以概括为：

- 写入路径：单 writer queue，后台串行 group commit
- Flush 路径：immutable queue + 单 worker
- Compaction 路径：单 worker
- 读取路径：在抓取快照后尽量锁外读取 immutable 和 SSTable

这种设计的好处是系统状态机更容易解释，Flush 与 Compaction 的行为边界更清楚，也更适合作为一个逐步演进的存储引擎项目。

## 七、测试覆盖

ForgeDB 当前已经建立了较系统的测试体系，覆盖范围包括：

### 1. WAL 层

- 单条记录与批量记录追加
- WAL 重放
- 截断尾记录恢复

### 2. Manifest 层

- 快照写入与读取
- 尾部损坏容忍
- Checkpoint 之后继续追加

### 3. MemTable 层

- `Put / Get / Overwrite / Delete`
- 范围扫描
- 返回值拷贝，避免暴露内部切片

### 4. SSTable 层

- 写表与点查
- 迭代器顺序扫描
- 稀疏索引边界
- Bloom miss 快速跳过
- Table Cache 复用与驱逐

### 5. DB 主路径

- WAL sync 三种模式
- Group commit
- Auto Flush
- Flush 队列与背压
- Flush/Reopen/Crash 恢复
- Compaction 规则与最终正确性
- Tombstone 语义
- 后台错误暴露
- 多层推进与文件清理

### 6. 黑盒集成测试

系统还提供了只通过公开 API 进行验证的集成测试，用于检查端到端的读写、刷盘、重启恢复、批量写和并发写行为。

## 八、项目特点与亮点

### 1. 不只是“组件拼装”，而是完整主路径闭环

ForgeDB 并不是简单实现了 WAL、MemTable、SSTable 这些单点模块，而是把它们串成了一条完整的存储引擎主线，包括前台写入、后台刷盘、后台压缩和崩溃恢复。

### 2. 强调工程边界而不是单纯追求功能数量

项目在写路径、Flush、Compaction 和恢复逻辑上都刻意保持职责边界清晰，例如 writer queue、immutable queue、pendingPaths、Manifest 快照等设计，都体现了对系统状态机的关注。

### 3. 具备较强的可解释性和可扩展性

当前实现虽然不是工业级成品，但整体结构清楚，便于继续扩展更正式的版本管理、更高并发度的后台任务、流式读路径和性能基线测试。

### 4. 恢复与测试意识较强

相比只关注读写功能的简单项目，ForgeDB 对崩溃恢复、文件遗留状态、后台错误暴露和恢复闭环有更完整的考虑，也配套编写了较系统的测试。

## 九、当前限制

ForgeDB 当前已经具备较完整的工程化原型特征，但仍然不是工业级最终产品。当前主要限制包括：

1. Manifest 仍是完整快照式模型，而不是更细粒度的 VersionEdit 日志模型。
2. Compaction 仍为单 worker，尚未实现并行压缩。
3. `Scan` 和 `Iterator` 仍偏向物化结果集，尚未完全演进为流式读路径。
4. benchmark 体系尚未系统建立，性能表现还缺少标准化评估。
5. 更复杂的并发语义，例如事务、MVCC、细粒度读写隔离，仍未引入。

## 十、后续计划

后续如果继续推进，ForgeDB 计划重点完善以下方向：

1. 建立 benchmark 体系，形成更清晰的吞吐、延迟与恢复性能基线。
2. 增强故障注入与恢复测试矩阵，进一步验证复杂交错场景下的一致性。
3. 继续演进 Version / Manifest 模型，提高版本切换与恢复机制的正式程度。
4. 将 `Scan / Iterator` 逐步改造成更高效的流式读路径。
5. 探索更高阶的并发控制方案与并行 Compaction 设计。

## 十一、适用场景

ForgeDB 当前适合作为：

- 自研数据库或存储引擎方向的学习与实践项目
- LSM-Tree、WAL、SSTable、Compaction 等核心机制的教学和展示样例
- 简历、面试、复试中展示系统能力和工程能力的项目
- 后续继续扩展为更完整单机存储内核的基础版本

## 十二、总结

ForgeDB 是一个围绕单机 LSM 存储引擎主线逐步构建的工程化项目。它当前已经实现了较完整的写路径、读路径、后台 Flush、层级 Compaction、恢复闭环与测试体系，整体完成度已经明显超过一般的课程作业或概念性原型。

虽然它仍然不是工业级数据库，但它已经具备了“高质量工程化存储引擎原型”的特征。对于一个自研数据库方向的个人项目来说，ForgeDB 的重点不只是实现功能，更在于通过清晰的结构、明确的状态边界和较完整的恢复逻辑，展示存储系统设计与实现的全过程。

