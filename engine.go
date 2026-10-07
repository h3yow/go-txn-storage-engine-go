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

	// 保留范围与增量回收相关状态。
	// historyFloor 是可打开历史视图的最低版本：来自最近一次完成的检查点。
	// gcSession 非空表示有一个跨多轮写入的分片检查点正在进行。
	// gcPrevFloor 是上一个已完成检查点的版本号，用于逐段判定回收：
	// 只删除“不含检查点分片、且段内最大提交版本 <= gcPrevFloor”的段。
	// 0 表示当前完成的是第一个检查点，本轮不删含数据段。
	retMu        sync.RWMutex
	historyFloor uint64
	gcSession    *gcSession
	gcPrevFloor  uint64
	gcRunning    sync.Mutex // 串行化所有（显式/自动）回收执行
	gcWake       chan struct{}
	autoStop     chan struct{}
	autoDone     chan struct{}

	// crashHook 仅用于测试：在回收的特定阶段被调用，钩子返回的退出码
	// 通过 os.Exit 模拟“回收到一半进程被强杀”（不运行 defer、不 fsync 收尾）。
	// phase 取值：见 retention.go 中的 crashPhase* 常量。
	crashHook func(phase string)

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
		gcWake:   make(chan struct{}, 1),
	}

	// 回放日志：若有检查点（整段式或分片式，取版本最高者），
	// 从检查点全量态开始，只应用检查点之后的提交。
	commits, cp, chunkCP, rstats, err := wlog.readAllRecords()
	if err != nil {
		return nil, err
	}
	bestCP := cp
	if chunkCP != nil && (bestCP == nil || chunkCP.version > bestCP.version) {
		bestCP = chunkCP
	}
	if bestCP != nil {
		db.store.restoreFromCheckpoint(bestCP.version, bestCP.state)
		db.version = bestCP.version
		db.historyFloor = bestCP.version
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

	// 初始保留边界：检查点版本即“可回看历史的最低版本”；
	// 若最高检查点处于“待最终化”中间态，下面会把边界回退到它之前。
	if bestCP != nil {
		db.historyFloor = bestCP.version
	}

	// 崩溃窗口恢复：回收删除采用“逐段内容判定”（见 deleteReclaimableSegments），
	// 天然幂等——重启后无法、也不需要精确区分“删段是否完成”，只需把版本最高
	// 的已完成分片检查点 S 恢复为：
	//
	//	historyFloor = S.version（保留边界随已落盘的末片立即生效，
	//	                             因为打开 S 视图所需的检查点分片都在）；
	//	gcPrevFloor  = 上一个完成检查点版本（没有则 0）；
	//
	// 若 S 末片之后崩溃、部分旧段尚未删除，它们在重启后第一次 GC 时会按
	// 同样的内容规则被幂等补删，删除结果与崩溃前应有的结果一致。
	// 写到一半（无 final）的会话不参与，其分片只占空间，不影响正确性。
	finSessions, ferr := wlog.finishedChunkSessions()
	if ferr != nil {
		return nil, ferr
	}
	if len(finSessions) > 0 {
		latest := finSessions[len(finSessions)-1]
		db.historyFloor = latest.version
		if len(finSessions) >= 2 {
			db.gcPrevFloor = finSessions[len(finSessions)-2].version
		}
	}

	if db.opts.EnableAutoGC && db.opts.RetainVersions > 0 {
		db.startAutoGC()
	}

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
	db.stopAutoGC()
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
	var t *Txn
	if readOnly {
		t = db.newReadOnlyTxnLocked(snap)
	} else {
		db.nextTxnID++
		t = &Txn{
			db:       db,
			id:       db.nextTxnID,
			readOnly: false,
			snapshot: snap,
			staged:   make(map[string]*writeEntry),
		}
		db.liveTxns[t.id] = t
	}
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
	db.signalAutoGC() // 老读者离开后，回收可能可以继续推进
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
	db.signalAutoGC()
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

// GCOptions 控制一次显式回收。零值字段表示沿用 Options 的配置。
type GCOptions struct {
	// MaxKeys/MaxBytes 覆盖本次调用的单轮工作量上限；0 表示用 Options 默认。
	MaxKeys  int
	MaxBytes int

	// MaxRounds 限制本次调用最多推进多少个有界小轮；0 表示推进到
	// 本轮无法继续为止（被保留边界/老读者卡住或已追平目标）。
	MaxRounds int
}

// GCResult 汇报回收结果。
type GCResult struct {
	Blocked          bool   // 是否因活跃旧快照而未执行回收（旧模式）
	OldestSnapshot   uint64 // 阻塞边界（Blocked 时有效）
	CheckpointVer    uint64 // 本次写入检查点的版本
	CheckpointSegSeq int    // 检查点所在新段序号
	SegmentsRemoved  int    // 删除的旧段数量

	// 增量回收模式（RetainVersions>0）的额外汇报。
	Incremental     bool   // 是否走了保留范围增量回收路径
	Rounds          int    // 本次实际推进的小轮数
	ChunksWritten   int    // 本次写入的检查点分片数
	KeysWritten     int    // 本次写入分片的键数
	Completed       bool   // 是否有一个检查点在本次调用中完成并推进了保留边界
	BlockedByReader bool   // 是否因活跃老读者暂时停在当前位置（可稍后自动继续）
	HistoryFloor    uint64 // 调用结束后当前保留边界
	RetainTarget    uint64 // 调用结束后保留策略理论目标
	EffectiveTarget uint64 // 综合老读者后的实际目标
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

	// 启用保留范围策略时走增量、有界的在线回收；默认路径行为完全不变。
	if db.opts.RetainVersions > 0 {
		return db.garbageCollectIncremental(gc)
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
