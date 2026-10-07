# go-txn-storage-engine

一个本地嵌入式、单机的事务型键值存储引擎（纯 Go，无第三方依赖）。
目标不是做一个通用数据库，而是把三件事的数据状态做到**可预期、能解释清楚**：

1. 读写事务与快照隔离；
2. 基于追加日志（WAL）的持久化与崩溃恢复；
3. 按历史提交点回看 + 保留范围内的在线增量日志回收。

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

// 历史视图：从某个历史提交点回看（只读）
view, err := db.BeginReadOnlyAt(7) // 钉在提交版本 v7
if errors.Is(err, storage.ErrSnapshotExpired) {
    // v7 已出保留范围，被回收了
}
v, _ := view.Get("user:1") // v7 那一刻的已提交状态
view.Rollback()            // 释放后回收才能越过 v7

// 省事说法：回看最近第 3 次提交
view2, _ := db.BeginReadOnlyLastN(3)
view2.Rollback()
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
| `ErrGCBlocked` | 整体式回收（`GarbageCollect`）被持有旧快照的活跃读事务阻塞 |
| `ErrSnapshotExpired` | 请求的历史提交点已出保留范围（历史已被回收或从未存在） |
| `ErrSnapshotInFuture` | 请求的历史提交点比最新已提交版本还新 |
| `ErrLogCorrupted` | 日志在**非尾部**位置损坏 |
| `ErrNotFound` | 快照视图下键不存在（含已删除） |
| `ErrClosed` | 引擎已关闭 |

`*LimitError` 还携带 `Size` / `Limit` 实际数值；`*GCBlockedError` 携带最老快照与拟回收版本；
`*SnapshotError` 携带请求版本、当前历史边界与最新版本。
“历史已过期 / 键不存在 / 事务已失效”三类原因两两可用 `errors.Is` 稳定区分。

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

整体式回收是**显式触发**的：调用 `db.GarbageCollect(storage.GCOptions{})`。
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

## 5. 历史视图与保留范围内的在线增量回收

在整体式 GC 之外，引擎提供一套**按历史提交点回看 + 有界增量回收**的能力，
让老读者不再把回收整体卡住。默认不启用（`RetainVersions=0`），行为与旧版完全一致。

### 历史视图语义

- `db.BeginReadOnlyAt(version)`：从指定提交版本开启只读视图。
  视图读到那一刻**已提交的完整状态**；之后任何提交、删除都不影响它；
  在该提交点之前被删除的键表现为 `ErrNotFound`。
- `db.BeginReadOnlyLastN(n)`：省事说法，“回看最近第 N 次提交”。
  `n=1` 即最新已提交版本；`n=0` 返回 `ErrSnapshotInFuture`。
- 视图用 `Rollback()` 结束。视图活跃期间被计入回收保护边界：
  **它依赖的历史不会被回收**（回收水位被最老活跃视图钳制，而不是拒绝回收）。
- 可回看的版本区间是 `[历史边界, 最新版本]`，落在区间外分别返回
  `ErrSnapshotExpired` / `ErrSnapshotInFuture`（`*SnapshotError` 携带边界细节）。

### 保留范围策略

`Options.RetainVersions = N` 表示“保留最近 N 个提交版本可回看”。
启用后（N>0）：

- 引擎启动**后台增量回收循环**：提交超出窗口、或读者/视图退出时自动唤醒推进；
- 也可以随时调用 `db.ReclaimLog()` 手动同步执行一轮（未启用保留策略时同样可用，
  此时目标水位为当前最新版本）。

### 增量回收的推进规则

每一轮回收做且只做一小步：

1. 目标水位 = 保留窗口允许回收到的版本（`当前版本 - N + 1`）；
2. 水位先被**最老活跃读者/视图快照**钳制——能安全收的先收，收不动的留着，
   读者结束后后台循环自动继续推进，不会卡住不动；
3. 再被 `ReclaimMaxVersionsPerRound` 钳制（单轮最多推进多少个版本）；
4. 把水位的全量态检查点写入一个**全新段**并 fsync，然后删除
   “提交记录全部 ≤ 水位”的非活动旧段；检查点字节数超过
   `ReclaimMaxCheckpointBytes` 时本轮跳过（单轮额外磁盘占用上限）。

单轮工作量与额外磁盘占用都有可配置上限，且每轮之间释放提交锁，
因此回收不会长时间独占提交路径，写入延迟不会出现明显尖峰。

### 崩溃安全与进度接续

- 检查点**先持久化、后删段**：回收到一半被强杀时，重启回放按版本号跳过
  ≤ 检查点的重复记录，已返回成功的提交一条不少；
- 历史边界（回收进度）就是检查点版本本身，重启后自动接续，不会回退；
- 删段动作幂等：重启后重跑回收不会把结果搞乱。

### 查询保留边界与回收进度

```go
info := db.RetentionInfo()
// info.CurrentVersion  最新已提交版本
// info.HistoryFloor    可回看的最早提交版本（历史边界）
// info.OldestReader    当前最老活跃读者快照（0 表示无）
// info.PendingReclaim  窗口之外仍有历史待回收
```

`db.ReclaimLog()` 返回的 `ReclaimResult` 携带本轮水位、边界推进、
删段数、检查点字节数、是否被读者钳制（`BlockedByReader`）与是否待续（`Pending`）。

### 与已有用法、已有数据目录的兼容

- `RetainVersions` 默认为 0：不启动后台回收、不做任何增量回收，
  `GarbageCollect` 等原有 API 行为完全不变；
- 已有数据目录无需迁移：无检查点的目录历史边界为 0（全部历史可回看）；
  曾被整体式 GC 处理过的目录，历史边界从已有检查点版本接续；
- 磁盘记录格式未变（复用既有的 COMMIT / CHECKPOINT 记录），新旧版本可互开目录。

---

## 6. 边界配置（`Options`）

| 字段 | 默认值 | 含义 |
| --- | --- | --- |
| `MaxKeyBytes` | 1 KiB | 单键最大字节数，超出返回 `ErrKeyTooLarge` |
| `MaxValueBytes` | 1 MiB | 单值最大字节数，超出返回 `ErrValueTooLarge` |
| `MaxTxnWriteBytes` | 16 MiB | 单事务累计写入上限，超出返回 `ErrTxnTooLarge` |
| `MaxWalSegmentBytes` | 4 MiB | 单 WAL 段大小，达到后追加写入自动滚动新段 |
| `SyncOnCommit` | `true` | 每次提交是否 fsync；崩溃恢复语义要求保持开启 |
| `RetainVersions` | 0（不启用） | 保留最近 N 个提交版本可回看；>0 时启动后台增量回收 |
| `ReclaimMaxVersionsPerRound` | 128 | 单轮回收水位最多推进多少个版本（0=默认） |
| `ReclaimMaxCheckpointBytes` | 64 MiB | 单轮回收检查点字节数上限，即单轮额外磁盘占用上限（0=默认） |

前四项上限必须为正数，否则 `Open` 直接失败；回收两项上限为 0 表示取默认值。

---

## 7. 本地运行与验证
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
| `TestHistoricalViewAtCommitPoint` | `history_test.go` | 按提交点开历史视图：视图固定那一刻的已提交状态，之后的提交/删除不可见，已删键表现为不存在 |
| `TestBeginReadOnlyLastN` | `history_test.go` | “回看最近第 N 次提交”的换算与 n=0/超限的边界错误 |
| `TestSnapshotExpiredDistinctErrors` | `history_test.go` | 历史过期 / 键不存在 / 事务失效三类原因用 `errors.Is` 两两区分 |
| `TestMultipleHistoricalViewsConcurrently` | `history_test.go` | 多个历史视图同时存活：并发读各自一致；回收被最老视图钳制并随视图释放逐段推进 |
| `TestReclaimRetentionBoundary` | `reclaim_test.go` | 保留窗口边界：窗口外过期、窗口内可看；重启后边界与数据接续 |
| `TestReclaimRoundBounds` | `reclaim_test.go` | 单轮推进版本数不超过 `ReclaimMaxVersionsPerRound` |
| `TestReclaimAutoResumesAfterReader` | `reclaim_test.go` | 后台回收被老读者钳制在某个点，读者结束后自动继续推进到窗口下沿 |
| `TestReclaimConcurrentWithCommits` | `reclaim_test.go` | 提交/回收/开视图三方并发（race 检测），已提交键一条不丢 |
| `TestReclaimCrashRecoveryLoop` | `reclaim_test.go` | 子进程并发提交+回收并反复 SIGKILL：已确认提交不丢、历史边界不回退、回收重来幂等 |

测试用 `-v` 运行时会打印关键输入（初始版本、暂存值、残帧字节数、
最老快照版本、提交点、保留窗口、回收水位、提交结果等）与判定依据
（赢家版本、截断位置、删除段数、边界推进），
并在恢复测试里显式输出“崩溃前 / 重启恢复后”的数据快照做对比。

---

## 8. 设计取舍与边界

- 数据在内存中保留完整多版本链，重启由 WAL/检查点重建；本实现面向“把状态与恢复语义讲清楚”，没有做块式索引或后台压缩。
- 提交点串行化（一把提交锁）换取了冲突检测与发布顺序的简单可靠；读路径无阻塞。
- 整体式 GC（`GarbageCollect`）采用保守安全策略：只要存在快照早于检查点版本的活跃事务就整体不回收，
  宁可少回收，也不删掉任何仍可能被读到的记录。
- 增量回收（`ReclaimLog` / 后台循环）则反过来追求“能收先收”：水位被最老读者钳制而非整体拒绝，
  单轮工作量与磁盘占用有上限，换得老读者不再卡住日志空间；代价是每轮要写一份水位全量态检查点，
  且段内版本混杂时（一段里既有 ≤ 水位又有 > 水位的提交）该段要等后续轮次才能回收。
- 历史视图依赖内存版本链 + 磁盘检查点共同保证：进程内视图由活跃事务集合保护，
  重启后可回看的范围以持久化的历史边界（检查点版本）为准。
