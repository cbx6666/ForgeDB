# MonolithDB

MonolithDB 是一个单机嵌入式的 LSM 风格 KV 存储引擎，使用 Go 实现。项目已经具备完整的写入、持久化、恢复、压缩和范围读取链路，适合课程项目展示、存储引擎学习和本地实验。

## 功能概览

- 支持 `Put / Get / Delete`
- 支持 `WriteBatch` 批量写入
- 支持 `Scan(start, end)` 范围读取
- 支持 `NewIterator(start, end)` 迭代访问
- 支持手动 `Flush()` 和基于阈值的自动 flush
- 支持后台 `RequestCompaction()`
- 支持 WAL group commit
- 支持 `WALSyncNever / WALSyncEveryBatch / WALSyncInterval`
- 支持 WAL 回放恢复
- 支持 `memtable + immutable memtable`
- 支持 SSTable 稀疏索引、Bloom filter、表级 key 范围持久化
- 支持 `MANIFEST`/Version 元数据恢复
- 支持 orphan SST 清理
- 支持交互式 demo 和端到端测试

## 项目结构

```text
monolithdb/
  monolithdb.go         # 对外公开 API
  README.md
  cmd/demo/             # 交互式演示程序
  internal/db/          # DB 主流程、flush、compaction、恢复
  internal/memtable/    # 内存表与 skiplist
  internal/sstable/     # SSTable 编解码、索引、Bloom、迭代器、元信息缓存
  internal/wal/         # WAL 读写与回放
  internal/types/       # 公共内部数据结构
```

### internal 目录说明

`internal/` 是引擎真正的实现主体，根包 `monolithdb` 只负责把这些内部能力整理成稳定的对外接口。

#### `internal/db`

这是数据库主控制层，负责把各个子模块串起来，核心职责包括：

- 管理数据库生命周期：打开、关闭、恢复、后台协程启动与退出
- 维护当前运行状态：可写 memtable、immutable memtable、当前 Version
- 组织前台读写路径：`Put / Get / Delete / WriteBatch / Scan / Iterator`
- 调度后台任务：auto flush、WAL interval sync、compaction
- 安装 flush 和 compaction 的结果，并更新 `MANIFEST`

可以把这一层理解成“调度中枢”。它自己不直接实现 skiplist、WAL 编码或 SST 文件格式，但决定了这些模块在什么时机被调用、以什么顺序协作。

#### `internal/memtable`

这一层负责内存中的有序写入结构，目前的核心是 skiplist。它的作用是：

- 承接最新写入的数据
- 以有序形式保存键值，方便后续 flush 成 SST
- 支持点查和范围遍历
- 支持 tombstone，表示删除语义

数据库运行时会同时出现两类 memtable：

- active memtable：当前可写的内存表
- immutable memtable：已经冻结、等待 flush 的内存表

这种设计让前台写入和后台 flush 可以衔接起来，而不需要在 flush 期间完全停写。

#### `internal/wal`

这一层负责预写日志，也就是所有写入的第一份持久化记录。它的职责包括：

- 追加 `Put/Delete` 记录
- 支持 batch 追加
- 在数据库启动时回放日志恢复 memtable

它和 `internal/db` 配合后形成完整写入语义：

- 先写 WAL
- 再把内容应用到 memtable

在当前实现里，WAL 已经支持 group commit 和多种 sync 策略，因此这一层不只是“能记录”，而是承担了持久化边界和恢复边界的核心责任。

#### `internal/sstable`

这一层负责磁盘上的有序表文件，也就是 LSM 的静态存储格式。它提供：

- SST 文件写入
- SST 点查
- 顺序迭代器
- 稀疏索引
- Bloom filter
- 表级 `minKey / maxKey`
- 表元信息缓存

这层的目标是让磁盘表既能被正确读取，也能在查询和 compaction 时尽量减少不必要的 IO。

例如：

- Bloom filter 用来快速排除“不可能命中”的文件
- 稀疏索引用来缩小数据扫描范围
- `minKey / maxKey` 用来判断范围是否重叠

#### `internal/types`

这一层放的是内部共享的数据结构，例如统一的 `Entry`。它让 WAL、memtable、SST、DB 主流程可以围绕同一种数据表达协作，减少模块间来回转换的复杂度。

### 模块之间的关系

如果按一条完整数据路径来看，这几个内部模块的关系可以概括成：

#### 写入

```text
internal/db
  -> internal/wal
  -> internal/memtable
  -> internal/sstable
```

也就是：

- `internal/db` 先把写入交给 WAL
- WAL 成功后再写入 memtable
- memtable 达到条件后再由 `internal/db` 触发 flush，把内容写成 SST

#### 读取

```text
internal/db
  -> internal/memtable
  -> internal/sstable
```

也就是：

- 先查 active memtable
- 再查 immutable memtable
- 最后按层级查询 SST

#### 恢复

```text
internal/db
  -> internal/wal
  -> MANIFEST / SST
```

也就是：

- 先根据 WAL 恢复最近写入
- 再根据 `MANIFEST` 恢复当前版本视图
- 必要时结合 SST 目录重建层级状态

## 公开 API

根包 `monolithdb` 对外暴露的主要接口如下：

- `Open(dir string) (*DB, error)`
- `OpenWithOptions(dir string, opt Options) (*DB, error)`
- `(*DB).Put(key, value)`
- `(*DB).Get(key)`
- `(*DB).Delete(key)`
- `(*DB).Write(batch *WriteBatch)`
- `(*DB).Flush()`
- `(*DB).RequestCompaction()`
- `(*DB).Scan(start, end)`
- `(*DB).NewIterator(start, end)`
- `DefaultOptions()`
- `Options.WithWALSync(mode, interval)`
- `Options.WithAutoFlushBytes(bytes)`

### 基本示例

```go
package main

import (
	"fmt"
	"log"
	"time"

	"monolithdb"
)

func main() {
	opt := monolithdb.DefaultOptions().
		WithWALSync(monolithdb.WALSyncInterval, 10*time.Millisecond).
		WithAutoFlushBytes(64 << 10)

	db, err := monolithdb.OpenWithOptions("./demo-data", opt)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := db.Put("user:1", []byte("alice")); err != nil {
		log.Fatal(err)
	}
	if err := db.Put("user:2", []byte("bob")); err != nil {
		log.Fatal(err)
	}
	if err := db.Delete("user:0"); err != nil {
		log.Fatal(err)
	}

	var batch monolithdb.WriteBatch
	batch.Put("user:3", []byte("carol"))
	batch.Put("user:4", []byte("dave"))
	batch.Delete("user:2")
	if err := db.Write(&batch); err != nil {
		log.Fatal(err)
	}

	if err := db.Flush(); err != nil {
		log.Fatal(err)
	}

	entries, err := db.Scan("user:", "user;")
	if err != nil {
		log.Fatal(err)
	}
	for _, ent := range entries {
		fmt.Printf("%s=%s\n", ent.Key, string(ent.Value))
	}
}
```

## 配置项

`DefaultOptions()` 会返回一份可修改的默认配置副本。

默认配置：

- 层级数为 3 层
- `WALSync` 默认使用 `WALSyncNever`
- `WALSyncInterval` 默认是 `10ms`
- 自动 flush 默认关闭

### WAL sync 策略

```go
opt := monolithdb.DefaultOptions().
	WithWALSync(monolithdb.WALSyncEveryBatch, 0)
```

支持三种模式：

- `WALSyncNever`：每个批次只做缓冲刷新
- `WALSyncEveryBatch`：每个批次写完后立即 `fsync`
- `WALSyncInterval`：由后台协程按固定间隔执行 `fsync`

### 自动 flush

```go
opt := monolithdb.DefaultOptions().
	WithAutoFlushBytes(64 << 10)
```

当 memtable 近似大小达到阈值时，系统会自动把当前可写 memtable 冻结成 immutable memtable，并在后台写出 L0 SST。

## 数据路径与读写流程

### 写入流程

`Put / Delete / WriteBatch` 的写入路径如下：

1. 前台请求进入写入队列
2. 后台 `writeLoop` 做短时间窗口聚合
3. 一批操作按顺序写入 WAL
4. 根据配置决定是否执行 `fsync`
5. WAL 成功后再按顺序应用到 memtable

这条路径保证：

- 成功返回的写入已经进入 WAL
- 成功返回的写入已经对 memtable 可见
- `WriteBatch` 内部的操作会按顺序应用

### 读取流程

`Get` 的查找顺序是：

```text
mem -> imm -> L0(newest-first) -> L1+
```

`Scan / Iterator` 会对多个来源做统一合并，处理覆盖关系，并过滤 tombstone。

### Flush

当调用 `Flush()` 或自动 flush 被触发时，流程如下：

1. 当前可写 memtable 冻结成 immutable memtable
2. 活跃 WAL 旋转为 flush WAL
3. 后台把 immutable memtable 写成 L0 SST
4. 安装新 Version
5. 清理 flush WAL

### Compaction

后台 compaction 会把较早层的 SST 合并到后一层：

- L0 输入允许重叠
- L1 及以后层级按 key 范围组织
- compaction 使用分段流式合并写出
- 到最后一层时会在写出阶段丢弃 tombstone

## 磁盘文件布局

数据库目录大致如下：

```text
demo-data/
  MANIFEST
  forge.wal
  forge.flush.wal
  sst/
    l0/
      000001.sst
      000002.sst
    l1/
      000003.sst
    l2/
      000004.sst
```

这些文件的含义：

- `MANIFEST`：当前版本视图，记录每层有哪些 SST 文件
- `forge.wal`：当前活跃 WAL
- `forge.flush.wal`：flush 中间态对应的 WAL
- `sst/l0/*.sst`：flush 直接生成的 SST
- `sst/l1+/*.sst`：compaction 生成的更后层 SST

### SSTable 元信息

每个 SST 会持久化以下内容：

- 数据区
- 稀疏索引
- Bloom filter
- 表级 `minKey / maxKey`
- footer

这些元信息会被读路径和 compaction 选择逻辑复用，用于：

- 快速判断 key 是否可能命中
- 缩小数据扫描范围
- 直接获取表级 key 范围

## 恢复能力

数据库启动时会完成以下恢复动作：

- 回放 `forge.wal`
- 在存在 flush 中间态时回放 `forge.flush.wal`
- 从 `MANIFEST` 恢复当前 Version
- 在需要时扫描 SST 目录重建层级信息
- 清理未被当前 Version 引用的 orphan SST

文件重命名、文件删除和关键目录项变更都会在对应路径上做目录级同步，以保证 flush、WAL 旋转和 compaction 安装过程的持久化语义。

## 交互式 Demo

直接运行：

```bash
go run ./cmd/demo
```

默认会把数据写到 `./demo-data`。也可以显式指定目录：

```bash
go run ./cmd/demo ./tmp/mydb
```

最常用命令：

```text
p a 1
g a
d a
s a z
i a z
f
c
ls
q
```

命令说明：

- `p <k> <v>`：写入一个键值
- `g <k>`：读取一个键
- `d <k>`：删除一个键
- `s [a] [b]`：扫描 `[a, b)` 范围
- `i [a] [b]`：迭代 `[a, b)` 范围
- `f`：手动 flush
- `c`：触发后台 compaction
- `ls`：查看当前目录下的 manifest、wal 和 sst 文件
- `q`：退出

## 测试

项目包含单元测试、集成测试和端到端测试。

常用命令：

```bash
go test ./...
```

端到端测试会直接使用公开 API 创建数据库目录，验证：

- 写入、删除、批量写、flush、scan、iterator
- 真实 WAL、MANIFEST、SST 文件生成
- 重启后的数据恢复
- compaction 进入更后层目录

## 适合展示的能力清单

如果你需要在答辩、报告或演示里概括项目，可以直接使用下面这份总结：

- 单机嵌入式 LSM KV 存储引擎
- 支持点写、点查、删除、批量写、范围读和迭代器
- WAL group commit 与可配置持久化策略
- memtable / immutable memtable / SST 多层组织
- 自动 flush、后台 compaction、Manifest 恢复
- Bloom filter、稀疏索引、SST key range 元信息
- 崩溃恢复、orphan 文件清理、目录级持久化同步
- 公开 API、端到端测试和交互式 demo
