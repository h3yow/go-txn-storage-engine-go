package storage

import (
	"fmt"
	"sync"
)

// DB 是嵌入式存储引擎。一把读写锁保护全局提交序列与读事务集合：
//   - 写事务 Commit 需要 Lock（串行化提交点，顺带完成冲突检测、落盘、发布）；
//   - 开启事务、Get（快照读）、GC 只需要 RLock。
//
// 磁盘上每个数据目录由 WAL 段文件构成；内存里是多版本键链。
type DB struct {
	dir  string
	opts Options

	wlog  *wal
	store *mvccStore

	commitMu sync.RWMutex
	version  uint64 // 当前最新已提交版本（单调递增）

	// liveTxns 记录当前所有活跃事务的快照版本，GC 据此确定不能越过的边界。
	liveMu    sync.Mutex
	liveTxns  map[uint64]*Txn
	nextTxnID uint64

	closedMu sync.RWMutex
	closed   bool

	// 最近一次启动恢复的统计，供诊断/测试验证残尾截断。
	recovery recoveryStats
}

// Open 打开（不存在则初始化）一个数据目录，执行崩溃恢复后返回可用引擎。
func Open(dir string, opts Options) (*DB, error) {
	if err := opts.validate(); err != nil {
		return nil, err
	}

	wlog, wstats, err := openWAL(dir, opts)
	if err != nil {
		return nil, err
	}

	db := &DB{
		dir:      dir,
		opts:     opts,
		wlog:     wlog,
		store:    newMVCCStore(),
		liveTxns: make(map[uint64]*Txn),
		recovery: wstats,
	}

	// 回放日志：若有检查点，从检查点全量态开始，只应用检查点之后的提交。
	commits, cp, rstats, err := wlog.readAllRecords()
	if err != nil {
		return nil, err
	}
	if cp != nil {
		db.store.restoreFromCheckpoint(cp.version, cp.state)
		db.version = cp.version
	}
	cpVer := db.version
	for _, rec := range commits {
		if rec.version <= cpVer {
			// 检查点已包含该版本，跳过。
			if rec.version > db.version {
				db.version = rec.version
			}
			continue
		}
		db.store.applyAt(rec.version, rec.writes)
		if rec.version > db.version {
			db.version = rec.version
		}
	}
	db.recovery.CommitRecords = rstats.CommitRecords
	db.recovery.CheckpointFound = rstats.CheckpointFound
	db.recovery.CheckpointVer = rstats.CheckpointVer

	return db, nil
}

// Close 干净关闭：fsync 日志。不影响已落盘数据。
func (db *DB) Close() error {
	db.closedMu.Lock()
	if db.closed {
		db.closedMu.Unlock()
		return ErrClosed
	}
	db.closed = true
	db.closedMu.Unlock()
	return db.wlog.close()
}

func (db *DB) checkOpen() error {
	db.closedMu.RLock()
	defer db.closedMu.RUnlock()
	if db.closed {
		return ErrClosed
	}
	return nil
}

// Begin 开启一个读写事务，其视图固定为“当前最新已提交版本”。
func (db *DB) Begin() (*Txn, error) {
	return db.beginTxn(false)
}

// BeginReadOnly 开启一个只读事务。它持有一个快照版本，
// 会阻止 GC 删除该快照仍需要的旧日志（直到 Rollback/关闭）。
func (db *DB) BeginReadOnly() (*Txn, error) {
	return db.beginTxn(true)
}

func (db *DB) beginTxn(readOnly bool) (*Txn, error) {
	if err := db.checkOpen(); err != nil {
		return nil, err
	}
	db.commitMu.RLock()
	snap := db.version
	db.commitMu.RUnlock()

	db.liveMu.Lock()
	db.nextTxnID++
	t := &Txn{
		db:       db,
		id:       db.nextTxnID,
		readOnly: readOnly,
		snapshot: snap,
		staged:   make(map[string]*writeEntry),
	}
	db.liveTxns[t.id] = t
	db.liveMu.Unlock()
	return t, nil
}

// finishTxn 把从事务活跃集合中摘除；幂等。
func (db *DB) finishTxn(t *Txn) {
	if t.done {
		return
	}
	t.done = true
	db.liveMu.Lock()
	delete(db.liveTxns, t.id)
	db.liveMu.Unlock()
}

// commitTxn 是提交的唯一入口，串行执行，保证冲突判定 + 落盘 + 发布的原子性顺序。
func (db *DB) commitTxn(t *Txn) error {
	if err := db.checkOpen(); err != nil {
		t.done = true
		return err
	}
	writes := t.stagedWrites()

	// 只读事务或空写事务：没有任何东西需要落盘，直接结束。
	if t.readOnly || len(writes) == 0 {
		db.finishTxn(t)
		return nil
	}

	db.commitMu.Lock()
	defer db.commitMu.Unlock()

	// 进入串行提交点后再次确认事务仍活跃。
	if t.done {
		return ErrTxnNotActive
	}

	// 冲突检测（first-committer-wins）：
	// 本事务快照之后，只要写集中任意键出现过更新的已提交版本就拒绝，
	// 不区分那个版本是写还是删——并发方确实改过这份数据。
	newVersion := db.version + 1
	for _, w := range writes {
		if winner := db.store.firstVersionAfter(w.key, t.snapshot); winner != 0 {
			db.finishTxn(t)
			return &ConflictError{Key: w.key, ReadSnap: t.snapshot, Winner: winner}
		}
	}

	// 先落日志并 fsync，成功后才在内存发布版本。
	// 这样“返回成功 == 崩溃后必可见”，且不存在内存有、日志无的窗口。
	rec := commitRec{version: newVersion, writes: writes}
	if err := db.wlog.appendPayload(encodeCommit(rec), db.opts.SyncOnCommit); err != nil {
		// 落盘失败不发布版本，事务中止，调用方可重试。
		db.finishTxn(t)
		return fmt.Errorf("storage: commit failed before durability: %w", err)
	}

	db.store.applyAt(newVersion, writes)
	db.version = newVersion
	db.finishTxn(t)
	return nil
}

// oldestActiveSnapshot 返回当前活跃事务中最老的快照版本；无活跃事务返回 0。
// 这是 GC 的安全边界：任何快照可能读到的日志版本都不能被回收。
func (db *DB) oldestActiveSnapshot() uint64 {
	db.liveMu.Lock()
	defer db.liveMu.Unlock()
	var oldest uint64
	for _, t := range db.liveTxns {
		if oldest == 0 || t.snapshot < oldest {
			oldest = t.snapshot
		}
	}
	return oldest
}

// GCOptions 控制一次显式回收（当前为保留字段，便于将来扩展阈值/触发策略）。
type GCOptions struct{}

// GCResult 汇报回收结果。
type GCResult struct {
	Blocked          bool   // 是否因活跃旧快照而未执行回收
	OldestSnapshot   uint64 // 阻塞边界（Blocked 时有效）
	CheckpointVer    uint64 // 本次写入检查点的版本
	CheckpointSegSeq int    // 检查点所在新段序号
	SegmentsRemoved  int    // 删除的旧段数量
}

// GarbageCollect 在不影响任何活跃读事务的前提下回收日志：
//  1. 取当前最新已提交版本做全量检查点；
//  2. 若存在活跃事务的快照版本 <= 检查点版本所依赖的最老需求，
//     保守地拒绝回收并返回 ErrGCBlocked（携带边界细节）；
//  3. 否则把检查点强制写入一个全新段开头，删除该段之前的所有旧段。
//
// 为什么有活跃老读者时不能删：重启恢复只能从检查点版本开始，
// 而老读者持有的是更旧快照，删除检查点之前的日志会让那条视图无从重建。
func (db *DB) GarbageCollect(gc GCOptions) (GCResult, error) {
	if err := db.checkOpen(); err != nil {
		return GCResult{}, err
	}

	db.commitMu.RLock()
	cpVersion := db.version
	state := db.store.stateAt(cpVersion)
	db.commitMu.RUnlock()

	oldest := db.oldestActiveSnapshot()

	// 边界保护：只要存在活跃事务，且它持有的快照可能依赖检查点之前的历史，
	// 就拒绝回收。快照为 0（事务开始于空库）或快照 >= cpVersion 时不阻塞。
	if oldest != 0 && oldest < cpVersion {
		return GCResult{Blocked: true, OldestSnapshot: oldest, CheckpointVer: cpVersion},
			&GCBlockedError{OldestSnapshot: oldest, WantReclaimUpTo: cpVersion}
	}

	segSeq, err := db.wlog.rotateIntoNewSegment(encodeCheckpoint(checkpointRec{
		version: cpVersion,
		state:   state,
	}))
	if err != nil {
		return GCResult{}, err
	}

	removed, err := db.wlog.deleteSegmentsBefore(segSeq)
	if err != nil {
		return GCResult{CheckpointVer: cpVersion, CheckpointSegSeq: segSeq}, err
	}
	return GCResult{
		CheckpointVer:    cpVersion,
		CheckpointSegSeq: segSeq,
		SegmentsRemoved:  removed,
	}, nil
}

// Version 返回当前最新已提交版本号（诊断/测试用）。
func (db *DB) Version() uint64 {
	db.commitMu.RLock()
	defer db.commitMu.RUnlock()
	return db.version
}

// RecoveryStats 返回最近一次启动的恢复统计（诊断/测试用）。
func (db *DB) RecoveryStats() recoveryStats { return db.recovery }

// Get 是自动提交模式的便捷读：每次调用看到最新已提交版本。
func (db *DB) Get(key string) ([]byte, error) {
	if err := db.checkOpen(); err != nil {
		return nil, err
	}
	t, err := db.BeginReadOnly()
	if err != nil {
		return nil, err
	}
	defer t.Rollback()
	return t.Get(key)
}

// Put 是自动提交模式的便捷写。
func (db *DB) Put(key string, value []byte) error {
	if err := db.checkOpen(); err != nil {
		return err
	}
	t, err := db.Begin()
	if err != nil {
		return err
	}
	if err := t.Put(key, value); err != nil {
		t.Rollback()
		return err
	}
	return t.Commit()
}

// Delete 是自动提交模式的便捷删除。
func (db *DB) Delete(key string) error {
	if err := db.checkOpen(); err != nil {
		return err
	}
	t, err := db.Begin()
	if err != nil {
		return err
	}
	if err := t.Delete(key); err != nil {
		t.Rollback()
		return err
	}
	return t.Commit()
}
