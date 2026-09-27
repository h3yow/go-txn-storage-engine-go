# go-txn-storage-engine

一个本地嵌入式、单机的事务型键值存储引擎（纯 Go，无第三方依赖）。
目标不是做一个通用数据库，而是把三件事的数据状态做到**可预期、能解释清楚**：

1. 读写事务与快照隔离；
2. 基于追加日志（WAL）的持久化与崩溃恢复；
3. 日志回收时对仍在读旧快照的事务所做的边界保护。

包名为 `storage`，引入路径：`github.com/h3yow/go-txn-storage-engine-go`。

---

## 1. 快速使用

```go
opts := storage.DefaultOptions()

db, err := storage.Open("./mydata", opts)
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// 自动提交的便捷 API
_ = db.Put("user:1", []byte("alice"))
v, _ := db.Get("user:1")

// 显式事务
tx, err := db.Begin()
if err != nil { log.Fatal(err) }
_ = tx.Put("a", []byte("1"))
_ = tx.Put("b", []byte("2"))
if err := tx.Commit(); err != nil {
    // err 可被明确分类，见第 4 节
}

// 只读事务：持有一致快照，同时参与 GC 的安全边界计算
ro, _ := db.BeginReadOnly()
defer ro.Rollback()
```

---

## 2. 隔离级别：快照隔离（Snapshot Isolation）

- 每个事务在 `Begin()` 那一刻固定一个**快照版本**（当时最新的已提交版本号）。
- 事务内所有读都只看得到版本号 `<= snapshot` 的数据，其他事务之后的提交对它不可见。
- 事务自己的写入对自身可见（read-your-writes），但在 `Commit` 成功前对其他事务不可见。
- 读不阻塞写、写不阻塞读：内存中每个键保留一条按版本升序的版本链，读按快照版本取链上的最后一个可见版本。

### 写写冲突（first-committer-wins）

提交点在引擎内部串行执行。提交时对写集中每个键检查：**从事务快照之后，该键是否出现过更新的已提交版本**（写入或删除都算）。

- 没有：分配新版本 → WAL 落盘并 fsync → 发布到版本链。
- 有：后提交的事务被拒绝，返回 `ErrWriteWriteConflict`，并携带 `*ConflictError`
  细节（冲突键、本事务快照、抢先提交方版本）。

因此两个事务改同一份数据时，**先提交者赢**。

### 不同失败原因明确可区分

调用方统一用 `errors.Is` / `errors.As` 判定，而不是匹配字符串：

| 错误 | 含义 |
| --- | --- |
| `ErrWriteWriteConflict` | 快照之后同键被并发事务抢先提交（first-committer-wins） |
| `ErrTxnNotActive` | 事务已提交/已回滚/因冲突已中止，或对只读事务执行写操作 |
| `ErrKeyTooLarge` | 单键字节数超过 `MaxKeyBytes` |
| `ErrValueTooLarge` | 单值字节数超过 `MaxValueBytes` |
| `ErrTxnTooLarge` | 单事务累计写入字节数超过 `MaxTxnWriteBytes` |
| `ErrGCBlocked` | 回收被持有旧快照的活跃读事务阻塞 |
| `ErrLogCorrupted` | 日志在**非尾部**位置损坏 |
| `ErrNotFound` | 快照视图下键不存在（含已删除） |
| `ErrClosed` | 引擎已关闭 |

`*LimitError` 还携带 `Size` / `Limit` 实际数值；`*GCBlockedError` 携带最老快照与拟回收版本。

---

## 3. 日志（WAL）与崩溃恢复约定

### 磁盘布局

数据目录由若干滚动段文件组成，按文件名序号排序即日志的时间顺序：

```
wal-000001.log
wal-000002.log
...
```

每个段由一连串帧组成，帧格式（大端序）：

```
+---------+------------+-----------+---------+
| magic 4B| payloadLen | payload   | crc32 4B|
| "WALF"  | uint32     | payloadLenB| IEEE   |
+---------+------------+-----------+---------+
```

CRC32 覆盖 `magic || payloadLen || payload`。payload 的第一个字节区分记录类型：

- `1`：**提交记录** `COMMIT(version, [(key, value, deleted-flag), ...])`；
- `2`：**检查点记录** `CHECKPOINT(version, 全量键值态)`。

### 持久化顺序（为什么已提交就一定不丢）

提交严格按下面顺序执行，且对外“返回成功”发生在最后：

1. 冲突检测通过，分配新的提交版本号；
2. 把提交记录追加到 WAL，并在 `SyncOnCommit=true`（默认）时 `fsync`；
3. 成功后才把新版本追加进内存版本链。

因此不存在“内存里有、日志里没有”的成功返回；进程在任意点被强杀：

- **已提交**（已 fsync）：重启回放后完整可见；
- **未提交 / 提交写盘到一半**：没有完整 COMMIT 记录，回放不到任何版本，
  不会留下半截写入，也不会产生半个版本。

### 启动恢复流程

1. 按序号读取每个段，逐帧校验 magic / 长度 / CRC。
2. **尾部半帧识别与自愈**：只允许最后一个段的末尾存在坏帧，识别后物理截断到最后一个完整帧的边界，引擎照常启动。尾部残帧包括三种形态：
   - 帧头或 CRC 不足；
   - 帧头完整但 payload 不完整；
   - 帧长度完整但 CRC 不匹配（极端写穿）。
3. **非尾部损坏不容忍**：坏帧之后若还存在有效日志（说明不是崩溃残尾，而是真正的介质/外部破坏），返回 `ErrLogCorrupted` 拒绝启动，避免静默丢数据。
4. 若存在检查点：以检查点的全量态 + 检查点版本重建内存，再顺序应用版本号大于检查点的提交记录；无检查点则从空态回放全部提交。
5. 恢复后以最新版本继续分配新版本号，版本序列单调连续。

恢复统计通过 `db.RecoveryStats()` 暴露：扫描段数、解析到的提交数、发现的检查点版本、被截断的残尾段名与字节数。

---

## 4. 日志回收（GC）与边界保护

### 触发方式

回收是**显式触发**的：调用 `db.GarbageCollect(storage.GCOptions{})`。
（段文件还会在单段大小超过 `MaxWalSegmentBytes` 时自动滚动，但滚动本身不删除旧段。）

一次回收做三件事：

1. 以当前最新已提交版本导出一份全量快照；
2. 强制把 `CHECKPOINT` 写入一个**全新段的开头**并 fsync；
3. 删除该段之前的所有旧段文件。

### 读事务保护规则

GC 前计算当前所有活跃事务中**最老的快照版本** `oldest`：

- 当 `oldest != 0 && oldest < checkpointVersion` 时，说明有读事务仍可能需要
  检查点之前的历史；GC **拒绝执行，不删除任何段**，返回 `ErrGCBlocked`
  （`*GCBlockedError` 带 `OldestSnapshot` / `WantReclaimUpTo`）；
- 该读事务 `Rollback()`（或结束）后再调用 GC，回收放行。

注意：保护针对的是**当前进程里仍活跃的内存视图**。重启后所有视图都从检查点/日志重建，因此检查点之前的段只在进程存活期间被旧读者需要。

### 检查点之后的恢复

重启时从检查点全量态开始，只回放检查点版本之后的提交，因此删掉旧段不影响
“已提交数据完整可见”这一保证；删除标记（墓碑）也包含在全量态中。

---

## 5. 边界配置（`Options`）

| 字段 | 默认值 | 含义 |
| --- | --- | --- |
| `MaxKeyBytes` | 1 KiB | 单键最大字节数，超出返回 `ErrKeyTooLarge` |
| `MaxValueBytes` | 1 MiB | 单值最大字节数，超出返回 `ErrValueTooLarge` |
| `MaxTxnWriteBytes` | 16 MiB | 单事务累计写入上限，超出返回 `ErrTxnTooLarge` |
| `MaxWalSegmentBytes` | 4 MiB | 单 WAL 段大小，达到后追加写入自动滚动新段 |
| `SyncOnCommit` | `true` | 每次提交是否 fsync；崩溃恢复语义要求保持开启 |

所有上限必须为正数，否则 `Open` 直接失败。

---

## 6. 本地运行与验证

要求 Go 1.25+（见 `go.mod`）。

```bash
# 编译
go build ./...

# 全部测试（含三类关键场景）
go test -v ./...

# 并发与数据竞争检查
go test -race ./...

# 反复跑，观察稳定性
go test -count=20 ./...
```

### 关键测试用例

| 测试 | 文件 | 覆盖点 |
| --- | --- | --- |
| `TestConcurrentWriteWriteConflict` | `txnconflict_test.go` | 8 个事务基于**同一快照**并发改同一键：恰 1 个成功，其余全部以 `ErrWriteWriteConflict` 拒绝；冲突后事务为 `ErrTxnNotActive`，两类原因可区分 |
| `TestSnapshotIsolationUnderConcurrentWrites` | `snapshot_concurrent_test.go` | 老读事务在写者持续提交期间反复读，视图恒为开事务时的值 |
| `TestGCBlockedByActiveReader` | `gc_test.go` | 老读事务期间 GC 被 `ErrGCBlocked` 阻塞且一段不删；读事务结束后 GC 成功，重启数据完整、版本继续增长 |
| `TestGCPreservesMultipleKeys` | `gc_test.go` | 检查点全量态（含墓碑）回收后完整保留 |
| `TestRealProcessKillDurability` | `crash_external_test.go` | 用独立子进程写入并 `SIGKILL` 强杀：已确认提交的数据重启完整，未完成的只能是完整值或不存在 |
| `TestRecoverTornTail` | `recovery_test.go` | 在日志尾部手工写入“只有帧头+半段 payload”的半帧：重启识别并截断，已提交数据恢复前后逐项对比一致，半截事务无版本、无数据；之后可继续追加并再次干净重启 |
| `TestRecoverTornTailCRCMismatch` | `recovery_test.go` | 长度完整但 CRC 被写坏的尾帧同样被截断跳过 |
| `TestCorruptionMidLogRejected` | `recovery_test.go` | 段中段损坏（坏帧后仍有有效段）时返回 `ErrLogCorrupted`，引擎拒绝启动 |
| `TestBoundaryRejections` | `limits_test.go` | 键/值/事务三类超限分别返回不同错误且携带实际尺寸，互不混淆 |

测试用 `-v` 运行时会打印关键输入（初始版本、暂存值、残帧字节数、
最老快照版本等）与判定依据（赢家版本、截断位置、删除段数），
并在恢复测试里显式输出“崩溃前 / 重启恢复后”的数据快照做对比。

---

## 7. 设计取舍与边界

- 数据在内存中保留完整多版本链，重启由 WAL/检查点重建；本实现面向“把状态与恢复语义讲清楚”，没有做块式索引或后台压缩。
- 提交点串行化（一把提交锁）换取了冲突检测与发布顺序的简单可靠；读路径无阻塞。
- GC 采用保守安全策略：只要存在快照早于检查点版本的活跃事务就整体不回收，
  宁可少回收，也不删掉任何仍可能被读到的记录。
