package storage

import "time"

// 本文件实现“保留范围内的增量在线回收”：
// 当 Options.RetainVersions > 0 时，GC 不再是“要么整体拒绝要么一把梭”，
// 而是把一份全量检查点切成有界小块，跨多轮写入；每轮工作量与额外
// 磁盘占用都有上限，提交路径不会被长时间独占。老读者只会把回收目标
// 暂时压低，读者离开后回收自动继续向前推进。

// gcSession 是一个跨多轮写入的分片检查点会话。
type gcSession struct {
	version      uint64   // 检查点目标版本（也是 session 标识）
	seq          uint32   // 下一个待写分片顺序号
	keys         []string // 开始会话时在目标版本下存在的键快照（固定顺序）
	idx          int      // 下一个待处理的 keys 下标
	startSegSeq  int      // 承载本会话首片的段序号（恢复装配起点）
	finalSegSeq  int      // 承载本会话末片的段序号（删段安全边界）
	finalWritten bool     // 末片是否已落盘；true 时仅差“确认无老读者→删旧段→推边界”
}

// oldestReaderVersion 返回当前活跃事务中最老的快照版本。
// 与 engine.oldestActiveSnapshot 不同：这里把“快照版本 == 当前保留边界”
// 的视图也算作普通读者，且调用方持有的语义更简单——返回 0 表示无活跃读者
// （快照 0 的读写事务开始于空库，不依赖任何历史，同样以 0 表示“不钉住”）。
func (db *DB) oldestReaderVersion() uint64 {
	db.liveMu.Lock()
	defer db.liveMu.Unlock()
	var oldest uint64
	for _, t := range db.liveTxns {
		if t.snapshot == 0 {
			continue // 空库快照不需要任何历史
		}
		if oldest == 0 || t.snapshot < oldest {
			oldest = t.snapshot
		}
	}
	return oldest
}

// gcTargets 在调用时刻计算回收的三个水位：
//   - current：当前最新已提交版本；
//   - retainTarget：仅按保留策略允许的检查点版本（current-RetainVersions）；
//   - effective：再扣除活跃老读者后的实际允许目标。
//
// 回收只允许推进到 effective：任何存活视图（普通只读事务或历史视图）
// 可能读到的版本都不能被越过。老读者离开后 effective 自然前移，
// 下一轮回收自动继续，不会永久卡住。
func (db *DB) gcTargets() (current, retainTarget, effective uint64) {
	db.commitMu.RLock()
	current = db.version
	db.commitMu.RUnlock()

	retain := db.opts.RetainVersions
	if retain <= 0 {
		// 未启用保留范围：不存在保留目标，调用方应走旧的整段检查点 GC 路径。
		return current, 0, 0
	}
	if uint64(retain) >= current {
		retainTarget = 0
	} else {
		retainTarget = current - uint64(retain)
	}
	effective = retainTarget
	if oldest := db.oldestReaderVersion(); oldest != 0 {
		// 读者钉在 snapshot S：检查点最多到 S（S 本身及之后可由
		// 检查点基线重建，S 之前的原始日志必须为它保留）。
		if oldest < effective {
			effective = oldest
		}
	}
	return current, retainTarget, effective
}

// RetentionStatus 汇报当前保留边界与回收推进位置（诊断/监控用）。
type RetentionStatus struct {
	// Enabled 是否启用了保留范围策略（Options.RetainVersions > 0）。
	Enabled bool
	// RetainVersions 配置的“保留最近多少个提交版本”。
	RetainVersions int

	// CurrentVersion 当前最新已提交版本。
	CurrentVersion uint64
	// HistoryFloorVersion 可打开历史视图的最低提交版本（保留边界）。
	// 它等于“最近一次完成检查点的版本”；重启后由日志中的检查点恢复。
	HistoryFloorVersion uint64
	// RetainTargetVersion 按保留策略理论上允许回收到的版本
	// （CurrentVersion - RetainVersions，下取），尚未考虑活跃读者。
	RetainTargetVersion uint64
	// EffectiveTargetVersion 综合保留策略与活跃读者后，本轮允许的检查点目标版本。
	EffectiveTargetVersion uint64
	// OldestReaderVersion 当前最老活跃视图（普通只读事务/历史视图）的版本；0 表示无。
	OldestReaderVersion uint64

	// SessionInProgress 是否存在写到一半的增量检查点。
	SessionInProgress bool
	// SessionVersion 当前进行中（或最近完成）的检查点目标版本。
	SessionVersion uint64
	// SessionChunksWritten 该检查点已写入的分片数。
	SessionChunksWritten int
	// SessionKeysTotal 该检查点计划包含的键总数（开始分片时确定）。
	SessionKeysTotal int
	// SessionKeysDone 已写入分片的键数。
	SessionKeysDone int

	// SegmentsRemoved 最近一次回收调用删除的旧段数。
	SegmentsRemoved int
}

// RetentionStatus 返回保留边界与回收进度快照。
func (db *DB) RetentionStatus() RetentionStatus {
	current, retainTarget, effective := db.gcTargets()
	floor := db.HistoryFloorVersion()

	db.retMu.RLock()
	sess := db.gcSession
	st := RetentionStatus{
		Enabled:                db.opts.RetainVersions > 0,
		RetainVersions:         db.opts.RetainVersions,
		CurrentVersion:         current,
		HistoryFloorVersion:    floor,
		RetainTargetVersion:    retainTarget,
		EffectiveTargetVersion: effective,
		OldestReaderVersion:    db.oldestReaderVersion(),
	}
	if sess != nil {
		st.SessionInProgress = true
		st.SessionVersion = sess.version
		st.SessionChunksWritten = int(sess.seq)
		st.SessionKeysTotal = len(sess.keys)
		st.SessionKeysDone = sess.idx
		if sess.finalWritten {
			// 末片已写完，键计数记为全部完成。
			st.SessionKeysDone = len(sess.keys)
		}
	}
	db.retMu.RUnlock()
	return st
}

// garbageCollectIncremental 是保留范围模式下的回收入口。
//
// 它把“一份全量检查点 + 删除旧段”这件重活拆成若干有界小轮：
// 每个小轮只落盘不超过 MaxKeys/MaxBytes 个键值，且键值逐个取短读锁，
// 不会长时间挡住提交；一轮结束立刻释放所有锁。
//
// 进行中的会话即使发现目标已偏旧也会把它写完（保证投入不浪费、
// 且崩溃后状态简单），写完后再按新水位开下一轮。因此回收永远
// 单调推进；老读者只会让“目标暂时长不高”，读者离开后自动追上。
func (db *DB) garbageCollectIncremental(gc GCOptions) (GCResult, error) {
	if err := db.checkOpen(); err != nil {
		return GCResult{}, err
	}

	// 显式 GC 与后台 GC 串行；不阻塞提交与开视图。
	db.gcRunning.Lock()
	defer db.gcRunning.Unlock()

	maxKeys, maxBytes := db.opts.gcRoundLimits()
	if gc.MaxKeys > 0 {
		maxKeys = gc.MaxKeys
	}
	if gc.MaxBytes > 0 {
		maxBytes = gc.MaxBytes
	}

	res := GCResult{Incremental: true}
	_, retainTarget, effective := db.gcTargets()
	res.RetainTarget = retainTarget
	res.EffectiveTarget = effective
	res.OldestSnapshot = db.oldestReaderVersion()
	res.HistoryFloor = db.HistoryFloorVersion()

	// 目标未越过当前边界：本轮什么都收不掉。
	if effective <= res.HistoryFloor {
		// 有理论回收空间但被活跃老读者压住时，明确标记以便调用方观察。
		res.BlockedByReader = retainTarget > res.HistoryFloor &&
			res.OldestSnapshot != 0 && effective < retainTarget
		return res, nil
	}

	for gc.MaxRounds == 0 || res.Rounds < gc.MaxRounds {
		done, err := db.gcOneRound(maxKeys, maxBytes, &res)
		if err != nil {
			return res, err
		}
		res.Rounds++
		_, retainTarget, effective = db.gcTargets()
		res.RetainTarget = retainTarget
		res.EffectiveTarget = effective
		res.HistoryFloor = db.HistoryFloorVersion()
		if done {
			// 一个检查点在本轮完成：水位允许就立刻接着开下一个。
			if effective <= res.HistoryFloor {
				return res, nil
			}
			continue
		}
		// 未完成分两种情况：
		//  1. 会话分片还在写——调用方限制了轮数（后台 GC）时返回；
		//     显式无限制调用继续把本轮会话写完。
		//  2. 被老读者挡在最终化——无论谁调用都只能返回，等读者离开
		//     的唤醒信号后自动/再次推进，不会永久卡住。
		db.retMu.RLock()
		sess := db.gcSession
		blockedFinal := sess != nil && sess.finalWritten
		db.retMu.RUnlock()
		if blockedFinal {
			return res, nil
		}
		if gc.MaxRounds > 0 {
			return res, nil
		}
	}
	return res, nil
}

// gcOneRound 推进一个有界小轮，返回 completed=true 表示本轮恰好写完
// 并提交了一个完整检查点（可能同时删除了旧段）。
func (db *DB) gcOneRound(maxKeys, maxBytes int, res *GCResult) (completed bool, err error) {
	db.retMu.Lock()
	sess := db.gcSession
	db.retMu.Unlock()

	// 特殊情形：检查点分片已全部落盘，只等挡路的老读者离开后最终化。
	if sess != nil && sess.finalWritten {
		return db.finalizeSession(sess, res)
	}

	if sess == nil {
		// 开启新会话：目标取当前允许水位的一个快照。
		_, _, effective := db.gcTargets()
		floor := db.HistoryFloorVersion()
		if effective <= floor {
			return false, nil // 水位未前移，无事可做
		}
		// 在 commit 写锁下原子地“固定目标版本 + 存活键清单 + 滚到新段写首片”，
		// 保证首片段滚动与并发提交的自动滚段不会互相穿插。
		db.commitMu.Lock()
		target := db.version
		if target > effective {
			target = effective
		}
		if target <= floor {
			db.commitMu.Unlock()
			return false, nil
		}
		keys := db.store.visibleKeysAt(target)
		sess = &gcSession{version: target, keys: keys}

		// 首片：空 entries + first 标记，强制写进一个全新段的开头。
		first := chunkCheckpointRec{
			session: target, version: target, seq: 0, first: true,
		}
		segSeq, werr := db.wlog.startChunkSession(encodeChunkCheckpoint(first))
		if werr != nil {
			db.commitMu.Unlock()
			return false, werr
		}
		sess.startSegSeq = segSeq
		sess.seq = 1
		db.commitMu.Unlock()

		db.retMu.Lock()
		db.gcSession = sess
		db.retMu.Unlock()
		res.ChunksWritten++
		db.crashAt(crashPhaseAfterFirstChunk)
	}

	// 收集本轮有界键值：逐个键取短读锁（getAt 自带 RLock），
	// 与正在进行的提交并发也没关系——每个值都取自会话固定的目标版本。
	var entries []writeEntry
	bytes := 0
	wouldFinish := false
	for {
		db.retMu.Lock()
		if sess.idx >= len(sess.keys) || len(entries) >= maxKeys || bytes >= maxBytes {
			wouldFinish = sess.idx >= len(sess.keys)
			db.retMu.Unlock()
			break
		}
		k := sess.keys[sess.idx]
		db.retMu.Unlock()

		v, gerr := db.store.getAt(k, sess.version)
		if gerr != nil {
			// 理论上不会发生（清单来自同一版本且进程内版本链不裁剪）；
			// 保守起见中止会话，等下一轮按新水位重开。
			db.retMu.Lock()
			db.gcSession = nil
			db.retMu.Unlock()
			return false, nil
		}
		entries = append(entries, writeEntry{key: k, value: v})
		bytes += len(k) + len(v)
		db.retMu.Lock()
		sess.idx++
		db.retMu.Unlock()
	}

	// 关键顺序：末片（final）一旦落盘，这份检查点就成为崩溃恢复基线。
	// 因此写末片前必须先确认没有更老的活跃读者；否则把这最后一批键
	// “退回”（idx 不动），本轮不写，会话保持未完成状态。读者离开会
	// 触发唤醒，下一轮写 final 并删段——不会堆积一串无法推进的会话。
	if wouldFinish {
		if oldest := db.oldestReaderVersion(); oldest != 0 && oldest < sess.version {
			db.retMu.Lock()
			sess.idx -= len(entries)
			db.retMu.Unlock()
			res.BlockedByReader = true
			res.OldestSnapshot = oldest
			return false, nil
		}
	}

	if wouldFinish {
		// 末片要强制滚动到一个全新段开头，这一步必须与提交串行，
		// 否则“滚动新段”可能和并发提交的自动滚动互相穿插，导致末片
		// 与后续提交落在错误的段里。临界区只含一次有界写+fsync，很短。
		db.commitMu.Lock()
		oldest := func() uint64 {
			db.liveMu.Lock()
			defer db.liveMu.Unlock()
			var o uint64
			for _, t := range db.liveTxns {
				if t.snapshot == 0 {
					continue
				}
				if o == 0 || t.snapshot < o {
					o = t.snapshot
				}
			}
			return o
		}()
		// 写末片前确认没有更老的活跃读者；被挡时把最后一批键退回，
		// 会话保持进行中，读者离开后自动继续。
		if oldest != 0 && oldest < sess.version {
			db.commitMu.Unlock()
			db.retMu.Lock()
			sess.idx -= len(entries)
			db.retMu.Unlock()
			res.BlockedByReader = true
			res.OldestSnapshot = oldest
			return false, nil
		}
		finalSeq, ferr := db.wlog.finalizeChunkInNewSegment(encodeChunkCheckpoint(chunkCheckpointRec{
			session: sess.version,
			version: sess.version,
			seq:     sess.seq,
			final:   true,
			entries: entries,
		}))
		if ferr != nil {
			db.commitMu.Unlock()
			return false, ferr
		}
		db.commitMu.Unlock()

		db.retMu.Lock()
		sess.finalSegSeq = finalSeq
		sess.seq++
		sess.finalWritten = true
		db.retMu.Unlock()
		res.ChunksWritten++
		res.KeysWritten += len(entries)
		db.crashAt(crashPhaseAfterFinalChunk)
		return db.finalizeSession(sess, res)
	}

	rec := chunkCheckpointRec{
		session: sess.version,
		version: sess.version,
		seq:     sess.seq,
		final:   false,
		entries: entries,
	}
	if err := db.wlog.appendPayload(encodeChunkCheckpoint(rec), true); err != nil {
		return false, err
	}
	db.retMu.Lock()
	sess.seq++
	db.retMu.Unlock()
	res.ChunksWritten++
	res.KeysWritten += len(entries)

	db.crashAt(crashPhaseMidChunks)
	return false, nil
}

// finalizeSession 在确认没有更老的活跃读者后，删除旧段并推进保留边界。
// 被挡住时保留会话（检查点已持久化），返回 completed=false，
// 等读者离开后的下一轮/后台唤醒再次尝试——回收不会永久停住。
//
// 锁顺序（由外到内）：commitMu（与提交串行）→ liveMu（算最老读者）
// → retMu（发布边界）。wal 删除内部还会取 wal.mu，与提交共用同一把
// WAL 锁，杜绝删段与滚动/追加互相穿插。
func (db *DB) finalizeSession(sess *gcSession, res *GCResult) (bool, error) {
	db.commitMu.Lock()
	defer db.commitMu.Unlock()

	db.liveMu.Lock()
	var oldest uint64
	for _, t := range db.liveTxns {
		if t.snapshot == 0 {
			continue
		}
		if oldest == 0 || t.snapshot < oldest {
			oldest = t.snapshot
		}
	}
	if oldest != 0 && oldest < sess.version {
		db.liveMu.Unlock()
		res.BlockedByReader = true
		res.OldestSnapshot = oldest
		return false, nil
	}
	db.liveMu.Unlock()

	db.retMu.Lock()
	prevFloor := db.gcPrevFloor
	db.retMu.Unlock()

	// 保留最新（本会话）与上一代两份完成检查点的全部分片；
	// 更老会话的残留分片可随段删除。session 标识即检查点版本号。
	keep := map[uint64]struct{}{sess.version: {}}
	if prevFloor > 0 {
		keep[prevFloor] = struct{}{}
	}

	removed, derr := db.wlog.deleteReclaimableSegments(prevFloor, keep)
	if derr != nil {
		return false, derr
	}
	db.crashAt(crashPhaseAfterDeleteSegs)

	db.retMu.Lock()
	if sess.version > db.historyFloor {
		db.historyFloor = sess.version
	}
	db.gcSession = nil
	db.gcPrevFloor = sess.version
	floor := db.historyFloor
	db.retMu.Unlock()

	res.SegmentsRemoved += removed
	res.Completed = true
	res.CheckpointVer = sess.version
	res.CheckpointSegSeq = sess.finalSegSeq
	res.HistoryFloor = floor
	return true, nil
}

// autoGCTickInterval 是后台回收在没有新事件时的兜底巡检间隔，
// 防止唤醒信号丢失导致回收永久静止。
const autoGCTickInterval = 50 * time.Millisecond

// 崩溃注入点标识（仅测试使用）。
const (
	crashPhaseAfterFirstChunk = "gc:after-first-chunk" // 会话首片刚落新段
	crashPhaseMidChunks       = "gc:mid-chunks"        // 中间分片写完后
	crashPhaseAfterFinalChunk = "gc:after-final-chunk" // 末片已 fsync、删段前
	crashPhaseAfterDeleteSegs = "gc:after-delete-segs" // 旧段已删、边界提交前
)

// crashAt 触发测试崩溃钩子（若安装）。
func (db *DB) crashAt(phase string) {
	if db.crashHook != nil {
		db.crashHook(phase)
	}
}

// startAutoGC 启动后台有界回收 goroutine。它只在 RetainVersions>0
// 且 EnableAutoGC 时由 Open 调用。
func (db *DB) startAutoGC() {
	db.autoStop = make(chan struct{})
	db.autoDone = make(chan struct{})
	go db.autoGCLoop()
}

// autoGCLoop 每轮执行至多一个有界小轮的回收，随后等待提交/读者结束唤醒
// 或兜底 tick；从不做大批量工作，也不会长期占用提交路径。
func (db *DB) autoGCLoop() {
	defer close(db.autoDone)
	ticker := time.NewTicker(autoGCTickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-db.autoStop:
			return
		case <-db.gcWake:
		case <-ticker.C:
		}
		if err := db.checkOpen(); err != nil {
			return
		}
		// 每轮只做一个有界小轮；MaxKeys/MaxBytes 沿用 Options 配置。
		_, _ = db.garbageCollectIncremental(GCOptions{MaxRounds: 1})
	}
}

// signalAutoGC 非阻塞地唤醒后台回收（信号槽容量为 1，合并提交风暴）。
func (db *DB) signalAutoGC() {
	if db == nil || db.gcWake == nil {
		return
	}
	if !(db.opts.EnableAutoGC && db.opts.RetainVersions > 0) {
		return
	}
	select {
	case db.gcWake <- struct{}{}:
	default:
	}
}

// stopAutoGC 停止后台 goroutine；未启用时为空操作。
func (db *DB) stopAutoGC() {
	if db.autoStop == nil {
		return
	}
	close(db.autoStop)
	<-db.autoDone
	db.autoStop = nil
}
