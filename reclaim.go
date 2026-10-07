package storage

// 本文件实现保留范围内的在线增量回收。
//
// 与 GarbageCollect（整体式、遇老读者即拒绝）不同，这里的回收是“有界轮次”：
// 每轮把历史边界（historyFloor）向保留窗口推进一小段——
//
//  1. 目标水位 = 保留窗口允许回收到的版本（未启用保留策略时为最新版本）；
//  2. 水位先被“最老活跃读者快照”钳制（能收的先收，收不动的留着），
//     再被 ReclaimMaxVersionsPerRound 钳制（单轮工作量上限）；
//  3. 把水位的全量态作为检查点写入一个全新段并 fsync，
//     然后删除“提交记录全部 <= 水位”的非活动旧段；
//     检查点字节数超过 ReclaimMaxCheckpointBytes 时本轮跳过（磁盘占用上限）。
//
// 崩溃安全：检查点先持久化、后删段；删到一半被强杀时，
// 重启回放按版本号跳过 <= 检查点的重复记录，下一轮重删是幂等的。
// 回收轮次与提交互斥（commitMu.Lock），但单轮工作量有界，
// 因此提交延迟的尖峰被限制在一轮检查点写入 + 有限次文件删除之内。

// ReclaimResult 汇报一轮增量回收的结果。
type ReclaimResult struct {
	FloorBefore      uint64 // 本轮前的历史边界
	FloorAfter       uint64 // 本轮后的历史边界（== 检查点版本，未推进时 == FloorBefore）
	Horizon          uint64 // 本轮钳制后的目标水位
	CheckpointSegSeq int    // 检查点写入的新段序号（未写时为 0）
	CheckpointBytes  int    // 检查点帧字节数
	SegmentsRemoved  int    // 本轮删除的旧段数量
	Skipped          bool   // 本轮是否无事可做（窗口内/被读者挡住/无可删段/检查点超限）
	Pending          bool   // 窗口之外是否仍有历史待回收（等读者退出或下一轮）
	BlockedByReader  uint64 // 钳制水位的最老活跃读者快照（0 表示未受读者限制）
}

// ReclaimLog 同步执行一轮有界增量回收，返回本轮推进情况。
// 未启用保留策略（RetainVersions == 0）时，目标水位为当前最新版本，
// 即“尽可能回收，但被活跃读者钳制”，可用于手动触发。
func (db *DB) ReclaimLog() (ReclaimResult, error) {
	if err := db.checkOpen(); err != nil {
		return ReclaimResult{}, err
	}
	// 手动触发与后台循环互斥，保证任意时刻只有一轮回收在进行。
	db.reclaimMu.Lock()
	defer db.reclaimMu.Unlock()

	// 与提交、历史视图开启互斥：水位计算、检查点导出、删段是一个原子轮次。
	db.commitMu.Lock()
	defer db.commitMu.Unlock()

	res := ReclaimResult{FloorBefore: db.historyFloor, FloorAfter: db.historyFloor}
	current := db.version
	floor := db.historyFloor

	// 目标水位：保留窗口允许回收到的位置。
	var target uint64
	if db.opts.RetainVersions == 0 {
		target = current
	} else if current >= db.opts.RetainVersions {
		target = current - db.opts.RetainVersions + 1
	}

	horizon := target
	// 读者钳制：任何活跃视图/读事务依赖的历史都不能收。
	if oldest := db.oldestActiveSnapshot(); oldest != 0 && oldest < horizon {
		horizon = oldest
		res.BlockedByReader = oldest
	}
	// 单轮推进上限：摊薄工作量，避免长时间独占提交路径。
	if maxStep := db.opts.ReclaimMaxVersionsPerRound; horizon > floor+maxStep {
		horizon = floor + maxStep
	}
	res.Horizon = horizon

	if horizon <= floor {
		res.Skipped = true
		res.Pending = target > floor
		return res, nil
	}

	// 没有“整段 <= 水位”的旧段时，写检查点回收不了任何空间，跳过。
	segs, err := db.wlog.deletableSegments(horizon)
	if err != nil {
		return res, err
	}
	if len(segs) == 0 {
		res.Skipped = true
		res.Pending = target > floor
		return res, nil
	}

	payload := encodeCheckpoint(checkpointRec{version: horizon, state: db.store.stateAt(horizon)})
	res.CheckpointBytes = len(payload)
	if int64(len(payload)) > db.opts.ReclaimMaxCheckpointBytes {
		// 单轮额外磁盘占用超限：本轮跳过，保留待续标记，下轮再试。
		res.Skipped = true
		res.Pending = true
		return res, nil
	}

	// 先持久化检查点（全新段开头 + fsync + 目录 fsync），再删旧段。
	segSeq, err := db.wlog.rotateIntoNewSegment(payload)
	if err != nil {
		return res, err
	}
	res.CheckpointSegSeq = segSeq
	// 检查点已落盘：历史边界即刻推进（即使随后的删段失败/进程被强杀，
	// 重启也能从该检查点恢复，删段动作下一轮幂等重放）。
	db.historyFloor = horizon
	res.FloorAfter = horizon

	removed, err := db.wlog.deleteSegmentsBySeq(segs)
	res.SegmentsRemoved = removed
	if err != nil {
		return res, err
	}
	res.Pending = target > horizon
	return res, nil
}

// reclaimLoop 是后台自动回收循环：被提交/读者退出唤醒后连续推进，
// 直到一轮都没有进展（被读者挡住或已达保留窗口）就睡回等待。
// 仅在 RetainVersions > 0 时启动。
func (db *DB) reclaimLoop() {
	defer db.reclaimWG.Done()
	for {
		select {
		case <-db.stopReclaim:
			return
		case <-db.wakeReclaim:
		}
		for {
			res, err := db.ReclaimLog()
			if err != nil || res.Skipped || res.FloorAfter <= res.FloorBefore {
				break
			}
			// 本轮有进展且可能还有窗口外历史：立刻继续下一轮。
			// 每轮之间释放 commitMu，提交可以插入，不会长时间被独占。
		}
	}
}

// kickReclaim 非阻塞地唤醒后台回收；未启用保留策略时是 no-op。
func (db *DB) kickReclaim() {
	if db.opts.RetainVersions == 0 || db.wakeReclaim == nil {
		return
	}
	select {
	case db.wakeReclaim <- struct{}{}:
	default: // 已有待处理的唤醒，合并
	}
}
