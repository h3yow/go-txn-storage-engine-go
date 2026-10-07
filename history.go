package storage

// 本文件实现“按历史提交点回看”的只读视图，以及保留范围/回收进度查询。
//
// 语义约定：
//   - 视图固定在指定提交版本上，读到那一刻已提交的完整状态；
//     之后其他事务的提交、删除都不影响它，被删除的键表现为 ErrNotFound。
//   - 可回看的版本区间是 [HistoryFloor, 当前最新版本]；
//     落在区间之外分别返回 ErrSnapshotExpired / ErrSnapshotInFuture，
//     与 ErrNotFound、ErrTxnNotActive 是不同错误类别。
//   - 视图活跃期间会被计入回收保护边界：它依赖的历史不会被回收。

// BeginReadOnlyAt 从指定历史提交点 version 开启一个只读视图。
// version 必须落在保留范围内（见 RetentionInfo）：
//   - version < 历史边界  → ErrSnapshotExpired（该段历史已被回收）
//   - version > 最新版本  → ErrSnapshotInFuture（该提交尚不存在）
//
// 返回的 *Txn 与 BeginReadOnly 一样，用 Rollback 结束并释放保护。
func (db *DB) BeginReadOnlyAt(version uint64) (*Txn, error) {
	if err := db.checkOpen(); err != nil {
		return nil, err
	}
	db.commitMu.RLock()
	defer db.commitMu.RUnlock()
	return db.beginViewLocked(version)
}

// BeginReadOnlyLastN 以“往前回看最近第 N 次提交”的方式开启只读视图：
// n=1 等价于当前最新已提交版本，n=2 是上一次提交，依此类推。
// n=0 视为指向“最新之后”的提交点，返回 ErrSnapshotInFuture；
// 回看点落在保留范围之前返回 ErrSnapshotExpired。
func (db *DB) BeginReadOnlyLastN(n uint64) (*Txn, error) {
	if err := db.checkOpen(); err != nil {
		return nil, err
	}
	db.commitMu.RLock()
	defer db.commitMu.RUnlock()

	current := db.version
	floor := db.historyFloor
	if n == 0 {
		return nil, &SnapshotError{Kind: ErrSnapshotInFuture, Want: current + 1, Floor: floor, Current: current}
	}
	var target uint64
	switch {
	case n <= current:
		target = current - n + 1
	case n == current+1:
		target = 0 // 第一次提交之前的空库状态
	default:
		// 回看点比第一次提交还早，必然落在保留范围之外。
		return nil, &SnapshotError{Kind: ErrSnapshotExpired, Want: 0, Floor: floor, Current: current}
	}
	return db.beginViewLocked(target)
}

// beginViewLocked 在 commitMu 持有（读或写）的前提下注册一个历史视图。
// 与增量回收互斥：回收推进历史边界需要 commitMu.Lock，
// 因此“检查边界 + 注册视图”对回收而言是原子的，视图一旦注册成功，
// 它依赖的历史就不会被回收掉。
func (db *DB) beginViewLocked(version uint64) (*Txn, error) {
	floor := db.historyFloor
	current := db.version
	if version < floor {
		return nil, &SnapshotError{Kind: ErrSnapshotExpired, Want: version, Floor: floor, Current: current}
	}
	if version > current {
		return nil, &SnapshotError{Kind: ErrSnapshotInFuture, Want: version, Floor: floor, Current: current}
	}

	db.liveMu.Lock()
	db.nextTxnID++
	t := &Txn{
		db:       db,
		id:       db.nextTxnID,
		readOnly: true,
		snapshot: version,
		staged:   make(map[string]*writeEntry),
	}
	db.liveTxns[t.id] = t
	db.liveMu.Unlock()
	return t, nil
}

// RetentionInfo 描述当前保留范围与回收进度（诊断/测试/监控用）。
type RetentionInfo struct {
	CurrentVersion uint64 // 当前最新已提交版本
	HistoryFloor   uint64 // 可回看的最早提交版本（0 表示从创世起全部保留）
	RetainVersions uint64 // 配置的保留窗口（0 表示未启用保留策略）
	OldestReader   uint64 // 当前最老活跃读者/视图的快照版本（0 表示无）
	PendingReclaim bool   // 保留窗口之外仍有历史未回收（被读者挡住或等下一轮）
}

// RetentionInfo 返回当前保留边界与回收推进状态。
func (db *DB) RetentionInfo() RetentionInfo {
	db.commitMu.RLock()
	current := db.version
	floor := db.historyFloor
	db.commitMu.RUnlock()

	info := RetentionInfo{
		CurrentVersion: current,
		HistoryFloor:   floor,
		RetainVersions: db.opts.RetainVersions,
		OldestReader:   db.oldestActiveSnapshot(),
	}
	if db.opts.RetainVersions > 0 && current >= db.opts.RetainVersions {
		// 保留窗口允许的历史下界比当前边界更新 → 还有可回收的。
		info.PendingReclaim = current-db.opts.RetainVersions+1 > floor
	}
	return info
}
