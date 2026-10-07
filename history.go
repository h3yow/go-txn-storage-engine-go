package storage

// 本文件实现“历史只读视图”：从指定历史提交点、或从最新点往回数第 N 次提交，
// 开启一个只读事务。视图语义与 BeginReadOnly 完全一致（快照隔离），
// 区别只在快照版本的来源，以及视图必须落在保留范围之内。
//
// 视图一旦开启就会注册进活跃事务集合：
//   - 它依赖的历史版本因此被当作 GC 安全边界的一部分，不能被回收越过；
//   - Rollback/Commit（只读事务 Commit 等价于结束）后自动释放。
//
// 与普通 ErrNotFound 的区别：本文件产生的失败全部是“视图开不起来”，
// 用 errors.Is(err, ErrHistoryNotAvailable) 即可与“键不存在”区分；
// 版本号本身非法时具体类别是 ErrInvalidVersion（同时仍命中前者）。

// BeginReadOnlyAt 从指定历史提交点 version 开启只读视图。
// 视图读到的恰为该提交点已提交的完整状态（含当时已删除键表现为不存在），
// 之后的提交对它不可见。
//
// 失败类别（errors.Is）：
//   - ErrInvalidVersion       version==0 或晚于当前最新提交；
//   - ErrHistoryNotAvailable  version 已落在保留边界之外；
//   - ErrClosed               引擎已关闭。
func (db *DB) BeginReadOnlyAt(version uint64) (*Txn, error) {
	if err := db.checkOpen(); err != nil {
		return nil, err
	}

	db.commitMu.RLock()
	current := db.version
	db.commitMu.RUnlock()

	if version == 0 || version > current {
		return nil, &HistoryError{
			Kind: ErrInvalidVersion, Version: version,
			Floor: db.HistoryFloorVersion(), Current: current,
		}
	}

	floor := db.HistoryFloorVersion()
	if version < floor {
		return nil, &HistoryError{
			Kind: ErrHistoryNotAvailable, Version: version,
			Floor: floor, Current: current,
		}
	}

	// 边界校验与注册必须对 GC 的“完成检查点”原子：
	// 锁序统一为 liveMu -> retMu，GC 完成检查点推进边界时持同样的锁序
	// 重算最老读者，因此要么本视图先注册（钉住边界），要么 GC 先推进
	// （本视图读到新边界并被拒绝），不存在“检查通过但历史已被清”的窗口。
	db.liveMu.Lock()
	db.retMu.RLock()
	floor = db.historyFloor
	db.retMu.RUnlock()
	if version < floor {
		db.liveMu.Unlock()
		return nil, &HistoryError{
			Kind: ErrHistoryNotAvailable, Version: version,
			Floor: floor, Current: current,
		}
	}
	t := db.newReadOnlyTxnLocked(version)
	db.liveMu.Unlock()
	return t, nil
}

// BeginReadOnlyBack 开启一个“往回看最近第 n 次提交”的只读视图：
//   - n=0 等价于 BeginReadOnly（最新提交点）；
//   - n=1 为倒数第二次提交；n=k 即 current-k 那个提交点。
//
// n 不能为负，且 n 不能超过当前提交数（current-n < 1）；
// 换算出的提交点若已超出保留范围，同样返回 ErrHistoryNotAvailable。
func (db *DB) BeginReadOnlyBack(n int) (*Txn, error) {
	if err := db.checkOpen(); err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, &HistoryError{
			Kind: ErrInvalidVersion, Version: 0,
			Floor: db.HistoryFloorVersion(), Current: db.Version(),
		}
	}

	db.commitMu.RLock()
	current := db.version
	db.commitMu.RUnlock()

	if uint64(n) >= current {
		// current-n <= 0：不存在那么早的提交点。
		return nil, &HistoryError{
			Kind: ErrInvalidVersion, Version: 0,
			Floor: db.HistoryFloorVersion(), Current: current,
		}
	}
	return db.BeginReadOnlyAt(current - uint64(n))
}

// newReadOnlyTxnLocked 在已持有 liveMu 时注册一个只读事务。
func (db *DB) newReadOnlyTxnLocked(snapshot uint64) *Txn {
	db.nextTxnID++
	t := &Txn{
		db:       db,
		id:       db.nextTxnID,
		readOnly: true,
		snapshot: snapshot,
		staged:   make(map[string]*writeEntry),
	}
	db.liveTxns[t.id] = t
	return t
}

// HistoryFloorVersion 返回当前可打开历史视图的最低提交版本（保留边界）。
func (db *DB) HistoryFloorVersion() uint64 {
	db.retMu.RLock()
	defer db.retMu.RUnlock()
	return db.historyFloor
}
