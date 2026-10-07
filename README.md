# go-txn-storage-engine

一个本地嵌入式、单机的事务型键值存储引擎（纯 Go，无第三方依赖）。
目标不是做一个通用数据库，而是把五件事的数据状态做到**可预期、能解释清楚**：

1. 读写事务与快照隔离；
2. 基于追加日志（WAL）的持久化与崩溃恢复；
3. 日志回收时对仍在读旧快照的事务所做的边界保护；
4. **按历史提交点回看数据的只读历史视图；**
5. **可配置保留范围内的增量在线回收（有界小步、老读者不整体阻塞、崩溃可续）。**

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

// 历史只读视图（需要在 Options.RetainVersions > 0 时使用）：
// 从历史提交点 v12 回看那一刻的完整已提交状态
hv, _ := db.BeginReadOnlyAt(12)
defer hv.Rollback()

// 或者更省事的写法：往回看最近第 2 次提交（0 表示最新）
hv2, _ := db.BeginReadOnlyBack(2)
defer hv2.Rollback()
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
| `ErrHistoryNotAvailable` | 请求的历史提交点已落在保留范围之外（被回收越过） |
| `ErrInvalidVersion` | 历史版本号本身非法（0、晚于当前、回看步数超界）；也命中 `ErrHistoryNotAvailable` |
| `ErrClosed` | 引擎已关闭 |

`*LimitError` 还携带 `Size` / `Limit` 实际数值；`*GCBlockedError` 携带最老快照与拟回收版本；`*HistoryError` 携带请求版本、当前保留边界 `Floor` 与当前版本 `Current`。

> “历史提交点不可用”与“该提交点下键不存在”是两个不同类别：开视图失败用
> `ErrHistoryNotAvailable`；视图成功打开后读一个当时不存在/已删除的键，
> 仍然是 `ErrNotFound`。调用方不需要解析错误文字即可区分。

---

## 2.5 历史只读视图

当 `Options.RetainVersions > 0` 时，可以从历史提交点开一个只读视图：

- `db.BeginReadOnlyAt(version)`：固定在**指定提交点**。视图读到的恰好是
  那一刻已经提交的完整状态——当时已删除的键表现为不存在（`ErrNotFound`），
  之后别的事务再提交也不影响它。
- `db.BeginReadOnlyBack(n)`：更省事的“往回看最近第 n 次提交”。
  `n=0` 等价于最新快照，`n=1` 是倒数第二次提交，依此类推。

视图语义与普通只读事务完全一致（快照隔离、read-only），区别只在快照版本的
来源。**只要视图还活着（未 `Rollback`/`Commit`），它依赖的历史就不会被回收
越过**：视图注册与回收“推进保留边界”在同一套锁下原子进行。

可开视图的版本区间是 `[HistoryFloorVersion, CurrentVersion]`。
- `version == 0`、`version > 当前版本`、`n < 0` 或回看步数超过已提交数：
  返回 `ErrInvalidVersion`（`errors.Is` 同时命中 `ErrHistoryNotAvailable`）。
- `version < HistoryFloorVersion`（保留范围之外）：返回 `ErrHistoryNotAvailable`。

`db.HistoryFloorVersion()` 与 `db.RetentionStatus()` 可随时查询当前保留边界、
保留目标、最老活跃读者、以及进行中的检查点进度。

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
- `2`：**检查点记录** `CHECKPOINT(version, 全量键值态)`（旧的整段回收路径使用，默认模式仍写这种）；
- `3`：**分片检查点记录** `CHUNK_CHECKPOINT(session, version, seq, first/final, entries)`
  （保留范围增量回收使用）。一次增量检查点由 `session` 相同、`seq` 从 0
  连续的多条记录组成：`first` 记录强制落在一个全新段开头，`final` 记录也
  强制独占一个全新段开头。只有首末齐全、`seq` 连续的会话才在恢复时生效；
  写到一半（无 `final`）的会话直接忽略，不影响正确性。

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

回收由 `db.GarbageCollect(storage.GCOptions{})` 显式触发；段文件还会在单段大小
超过 `MaxWalSegmentBytes` 时自动滚动（滚动本身不删除旧段）。引擎提供**两种回收
模式**，由 `Options.RetainVersions` 选择，默认行为完全保持兼容。

### 4.1 默认模式（`RetainVersions == 0`，旧行为不变）

一次回收做三件事：

1. 以当前最新已提交版本导出一份全量快照；
2. 强制把整段 `CHECKPOINT` 写入一个**全新段的开头**并 fsync；
3. 删除该段之前的所有旧段文件。

GC 前计算当前所有活跃事务中**最老的快照版本** `oldest`：

- 当 `oldest != 0 && oldest < checkpointVersion` 时，说明有读事务仍可能需要
  检查点之前的历史；GC **拒绝执行、不删除任何段**，返回 `ErrGCBlocked`
  （`*GCBlockedError` 带 `OldestSnapshot` / `WantReclaimUpTo`）；
- 该读事务 `Rollback()`（或结束）后再调用 GC，回收放行。

### 4.2 保留范围增量模式（`RetainVersions > 0`，新能力）

该模式表达“**保留最近 N 个提交版本可回看**”，并把回收从“要么整体拒绝、要么
一把梭”改为**有界、增量、可在线推进**：

- **有界小步**：一份全量检查点被切成多条分片记录（记录类型 3），跨多次 GC
  调用写入。单轮落盘的键数 / 字节数由 `MaxGCPerRoundKeys` / `MaxGCPerRoundBytes`
  限制；`GCOptions.MaxRounds` 限制单次调用推进的小轮数（后台 GC 恒为 1）。
  分片逐个键取短读锁，**不长时间独占提交路径**，也不会造成明显写入尖峰。
- **老读者不整体阻塞**：回收目标 = `当前版本 - RetainVersions`，再被“最老活跃
  读者快照”压低。能安全收的先收（把检查点推进到读者钉住的位置），暂时收不动的
  留着；**写最后一片之前会再确认一次没有更老读者**，被挡就把这一小批退回、
  不写末片。读者结束会唤醒回收（显式或后台），自动继续向前，**不会永久卡住**。
- **后台自动回收**：`EnableAutoGC: true` 时引擎启动一个后台 goroutine，
  每次提交/读者结束只做一次非阻塞唤醒；后台每轮只写一个有界分片，
  兜底每 50ms 巡检一次，避免唤醒信号丢失导致静止。
- **进度可见**：`db.RetentionStatus()` 返回当前版本、保留边界
  `HistoryFloorVersion`、保留目标、有效目标（含读者压制）、最老读者版本，
  以及进行中会话的目标版本 / 已写分片 / 键进度；`GCResult` 汇报每轮的
  `ChunksWritten` / `KeysWritten` / `Completed` / `SegmentsRemoved` 等。

### 4.3 增量模式为什么崩了也不乱：两代保留 + 逐段判定

末片（`final`）在 `commitMu` 下**原子地滚动到一个全新段开头**写入，所以末片
之后的提交都落在末片段或更后的段。删除旧段不是按段序号粗删，而是**逐段扫描
内容**，只删除同时满足以下条件的段：

1. 不是当前活动段；
2. 不含“最新”或“次新”两份完成检查点的任何分片；
3. 段内最大提交版本号 `<= 次新检查点版本`（这些提交已被上一代检查点完整覆盖）。

因此任何时刻磁盘上都保留着**最近两代检查点 + 其间全部提交**。即使进程在
“末片 fsync 后、删段中途”被强杀，重启后：

- 已返回成功的提交从某一代完整检查点连续重放，**一条都不会少**；
- 保留边界取日志中版本最高的已完成检查点（其分片都已 fsync）；
- 尚未删完的旧段在重启后第一次 GC 时被**幂等补删**，删多删少结果都一致；
- 写到一半（无 `final`）的会话直接忽略，只是暂时多占空间。

### 4.4 检查点之后的恢复

重启时从（版本最高的）检查点全量态开始，只回放检查点版本之后的提交，
因此删掉旧段不影响“已提交数据完整可见”这一保证；删除标记（墓碑）也包含在
全量态中。分片检查点恢复时，首片段已被旧回收删除的老会话其残留分片会被
安全忽略（不当作日志损坏）。

---

## 5. 边界配置（`Options`）

| 字段 | 默认值 | 含义 |
| --- | --- | --- |
| `MaxKeyBytes` | 1 KiB | 单键最大字节数，超出返回 `ErrKeyTooLarge` |
| `MaxValueBytes` | 1 MiB | 单值最大字节数，超出返回 `ErrValueTooLarge` |
| `MaxTxnWriteBytes` | 16 MiB | 单事务累计写入上限，超出返回 `ErrTxnTooLarge` |
| `MaxWalSegmentBytes` | 4 MiB | 单 WAL 段大小，达到后追加写入自动滚动新段 |
| `SyncOnCommit` | `true` | 每次提交是否 fsync；崩溃恢复语义要求保持开启 |
| `RetainVersions` | `0` | 保留最近多少个提交版本可回看。`0`=不启用，维持旧 GC 行为；`>0`=开启历史视图与增量在线回收 |
| `MaxGCPerRoundKeys` | 4096 | 增量回收单轮最多落盘的键数（`<=0` 取默认） |
| `MaxGCPerRoundBytes` | 1 MiB | 增量回收单轮最多落盘的字节数（`<=0` 取默认） |
| `EnableAutoGC` | `false` | 为 `true` 且 `RetainVersions>0` 时启动后台有界自动回收 |

所有尺寸上限必须为正数，`RetainVersions` / 两个 GC 单轮上限不得为负，否则
`Open` 直接失败。

### 与现有用法 / 已有数据目录的兼容性

- **默认行为不变**：不显式设置 `RetainVersions`（保持 0）时，`Begin/Commit/
  GarbageCollect/Get/Put/Delete` 的语义与旧版本逐条一致；`GCOptions{}` 仍走
  整段检查点 + 老读者 `ErrGCBlocked` 的旧路径，原有调用方无需任何改动。
- **旧数据目录可直接打开**：旧目录里只有类型 1/2 记录，新代码按原样回放。
  旧目录在默认模式下继续写类型 2 检查点；只有把 `RetainVersions` 调为 >0 后，
  后续 GC 才会产生类型 3 分片检查点。
- 反向不保证：一旦增量模式写入了类型 3 记录，旧版本程序不认识该类型，
  会拒绝打开（这是有意的前向保护）。
- 历史视图能力以“保留边界”为准：从未做过增量回收时 `HistoryFloorVersion`
  为 0，进程内仍可对任意 `<= 当前版本` 的版本开视图；一旦边界随回收前移，
  边界之前的版本按 `ErrHistoryNotAvailable` 拒绝。

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
| `TestHistoryViewMultipleReaders` | `history_retention_test.go` | 多个不同提交点的历史视图同时存在：各自视图稳定、删除键表现为不存在、未来提交不可见；视图存活时回收无法越过最老视图 |
| `TestHistoryViewErrorClassification` | `history_retention_test.go` | 非法版本 / 超出保留范围 / 键不存在三类错误稳定可区分，`*HistoryError` 携带边界 |
| `TestRetentionBoundaryAndRestart` | `history_retention_test.go` | 保留边界按 `current-N` 多轮推进、墓碑历史视图正确；干净重启后已提交一条不少、边界接续、可继续提交与回收 |
| `TestRetentionBoundedRounds` | `history_retention_test.go` | `MaxRounds`/单轮键数上限生效，一次调用不会一把写完，会话跨轮续做 |
| `TestIncrementalGCReaderDoesNotBlockForever` | `history_retention_test.go` | 老读者期间回收到读者位置即停、不越界；读者结束后自动续收到保留目标 |
| `TestAutoGCAdvancesInBackground` | `history_retention_test.go` | `EnableAutoGC` 下只提交不显式 GC，后台回收自动推进边界 |
| `TestDefaultOptionsUnchangedCompatibility` | `history_retention_test.go` | 默认配置走旧整段检查点路径，水位字段为零，旧用法不变 |
| `TestRetentionCrashRestartLoop` | `retention_crash_test.go` | 独立子进程内“提交与增量回收并发”，在首片后/中间分片后/末片后/删段后四个阶段反复强杀重启：逐轮核对已确认提交一条不少、版本不丢、边界可续、可继续提交 |

测试用 `-v` 运行时会打印关键输入（初始版本、提交点、保留范围、回收水位、
残帧字节数、最老快照版本等）与判定依据（赢家版本、截断位置、删除段数、
检查点/分片进度），并在恢复测试里显式输出“崩溃前 / 重启恢复后”的数据快照
做逐轮对比。

---

## 7. 设计取舍与边界

- 数据在内存中保留完整多版本链，重启由 WAL/检查点重建；本实现面向“把状态与恢复语义讲清楚”，没有做块式索引或后台压缩。
- 提交点串行化（一把提交锁）换取了冲突检测与发布顺序的简单可靠；读路径无阻塞。
- **默认 GC** 采用保守安全策略：只要存在快照早于检查点版本的活跃事务就整体不回收，宁可少回收，也不删掉任何仍可能被读到的记录。
- **增量 GC**（`RetainVersions>0`）用“有界分片 + 两代保留 + 逐段内容判定”
  换取在线回收能力：单轮工作量与额外磁盘占用可配、老读者只压低目标不整体
  阻塞、删除中途崩溃靠两代检查点幂等恢复。代价是任何时刻至少保留最近两代
  检查点及其间日志，空间回收滞后一轮（一个保留窗口）——这是用一点点空间
  换取“强杀后已确认提交一条不丢、回收重来结果不乱”的确定性。
- 历史视图的可回看区间严格等于 `[保留边界, 当前版本]`；边界随增量回收单调
  前移，进程重启后从日志中的已完成检查点重建，不会回退。

