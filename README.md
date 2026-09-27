# go-txn-storage-engine

本地嵌入式 Go 存储引擎，把**事务、持久化、崩溃恢复**三件事的数据状态做得可预期、可解释：

- 快照隔离（Snapshot Isolation）的读写事务
- 仅追加（append-only）提交日志 + fsync 的持久化
- 崩溃恢复：识别并截断损坏的日志尾部
- 感知活跃快照的日志回收（GC）
- 边界保护：键/值/事务超限分别返回可区分的错误

## 快速上手

```go
eng, _ := engine.Open(engine.Options{Dir: "./data"})
defer eng.Close()

tx, _ := eng.Begin(false)      // false = 读写事务
tx.Put("k", []byte("v")) // Put(key string, value []byte)
err := tx.Commit()             // 返回前已写日志并 fsync

var ce *engine.ConflictError
if errors.As(err, &ce) {       // 写写冲突：可拿到冲突键
    log.Println("conflict on", ce.Key)
}
```

## 隔离级别：快照隔离

- `Begin` 时事务拿到当前最大提交时间戳作为**快照点**，整个事务期间读到的是该时刻的一致视图；并发写事务的提交对已有读事务不可见。
- 写操作先缓冲在事务内，自己的写对自己可见（read-your-writes）。
- 提交时做**写写冲突检测**（first-committer-wins）：写集合中任一键在本事务快照点之后被其他事务提交过，则整个事务被拒绝，返回 `*ConflictError`（携带冲突键，用 `errors.As` 提取）。
- 错误原因可区分：`ConflictError`（写写冲突）、`ErrTxnClosed`（事务已提交/回滚后再操作）、`ErrEngineClosed`（引擎已关闭）互不相同。

## 日志与崩溃恢复约定

- 每次 `Commit` 先把该事务的全部写入编码为**一条**日志记录追加到 `wal.log` 并 `fsync`，然后才应用到内存视图——已提交事务要么完整可见，要么完全不可见，不存在半截事务。
- 记录格式：`[payloadLen uint32][crc32 uint32][payload]`，payload 内含 commitTS 与全部写操作。
- 重启时顺序扫描日志：遇到**长度读不全**或 **CRC 校验不符**的尾部，视为崩溃留下的半条记录，直接截断该尾部后照常启动；截断后日志可继续追加。
- 恢复以日志为准重放所有完整记录，重建多版本数据；未提交的事务从未写入日志，自然不会留下任何版本。

## 日志回收（GC）

- 触发条件：调用 `GC()` 手动回收；或用 `MaybeGC()` 在累计提交数达到 `Options.GCThreshold` 时自动回收。
- 回收边界：取**当前最老活跃读事务的快照点** `minSnapshot`，每个键只保留 `commitTS > minSnapshot` 的版本和 `<= minSnapshot` 的最新一个版本——活跃读事务的视图不会被回收破坏。
- 回收方式：内存裁剪旧版本后，把存活版本整体重写为新日志文件（先写 `.tmp` 再 rename，崩溃时旧文件仍完整可用）。

## 边界保护

| 超限场景 | 错误 |
|---|---|
| key 长度 > `MaxKeySize` | `ErrKeyTooLarge` |
| value 长度 > `MaxValueSize` | `ErrValueTooLarge` |
| 单事务写键数 > `MaxTxnWrites` | `ErrTxnTooLarge` |
| 键不存在 | `ErrNotFound` |
| 事务已结束再操作 | `ErrTxnClosed` |

均为独立的 sentinel error，可用 `errors.Is` 逐一区分。

## 本地验证

```bash
go build ./...
go test ./... -v        # 用例会打印关键输入与判定依据
go test -race ./...     # 竞态检测
```

测试覆盖：

- `TestConcurrentCommitConflict`：两事务并发改同一键，恰好一个成功、一个 `ConflictError`，并与 `ErrTxnClosed` 区分
- `TestGCDuringActiveReadTxn`：读事务活跃期间执行 GC，对比 GC 前后及重启后的读结果
- `TestTornTailRecovery`：手工在日志尾部拼半条记录，重启后对比恢复前后的数据
- `TestBoundaryErrors`：三类超限分别返回各自错误
- `TestSnapshotIsolation`：读事务期间并发写不可见
