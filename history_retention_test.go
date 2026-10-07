package storage

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// retentionOptions 构造“小段 + 保留 N 个版本 + 极小单轮”的配置，
// 让一次检查点必然跨多个小轮、多段，充分压到增量路径。
func retentionOptions(retain, maxKeys int) Options {
	o := smallSegmentOptions()
	o.RetainVersions = retain
	o.MaxGCPerRoundKeys = maxKeys
	o.MaxGCPerRoundBytes = 64 // 刻意压小：单轮通常只能写 1~2 个键
	return o
}

// runGCUntil 反复显式回收，直到 cond 成立或轮数耗尽。
func runGCUntil(t *testing.T, db *DB, cond func(RetentionStatus) bool, maxCalls int) RetentionStatus {
	t.Helper()
	var st RetentionStatus
	for i := 0; i < maxCalls; i++ {
		res, err := db.GarbageCollect(GCOptions{})
		if err != nil {
			t.Fatalf("GC 第 %d 次调用失败: %v", i, err)
		}
		st = db.RetentionStatus()
		if res.Completed {
			t.Logf("[回收水位] 完成检查点 v%d，删除旧段=%d，边界推进到 v%d",
				res.CheckpointVer, res.SegmentsRemoved, st.HistoryFloorVersion)
		}
		if cond(st) {
			return st
		}
	}
	return st
}

// TestHistoryViewMultipleReaders 覆盖“多个历史视图同时存在”：
// 不同提交点的视图各自稳定，互不可见对方之后的提交，删除键表现为不存在，
// 且所有存活视图都钉住各自的历史不被回收越过。
func TestHistoryViewMultipleReaders(t *testing.T) {
	opts := retentionOptions(2, 2)
	dir := filepath.Join(t.TempDir(), "db")
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// v1: a=1,b=1 / v2: a=2 / v3: b 删除 / v4: c=4 / v5: a=5
	mustCommit := func(fn func(*Txn) error) {
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		if err := fn(tx); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	mustCommit(func(tx *Txn) error {
		if err := tx.Put("a", []byte("v1-a")); err != nil {
			return err
		}
		return tx.Put("b", []byte("v1-b"))
	})
	mustCommit(func(tx *Txn) error { return tx.Put("a", []byte("v2-a")) })
	mustCommit(func(tx *Txn) error { return tx.Delete("b") })
	mustCommit(func(tx *Txn) error { return tx.Put("c", []byte("v4-c")) })
	mustCommit(func(tx *Txn) error { return tx.Put("a", []byte("v5-a")) })
	t.Logf("[输入] 已提交到 v%d（保留范围=%d，单轮键上限=2）", db.Version(), opts.RetainVersions)

	v1, err := db.BeginReadOnlyAt(1)
	if err != nil {
		t.Fatal(err)
	}
	v3, err := db.BeginReadOnlyBack(2) // current=5 → v3
	if err != nil {
		t.Fatal(err)
	}
	v5, err := db.BeginReadOnly() // 最新
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("[输入] 同时开三个视图: at(v1) / back(2)=v3 / latest=v%d", v5.snapshotVersion())

	// 视图内容逐项校验（含删除语义、未来提交不可见）。
	check := func(tx *Txn, want map[string]string) {
		for k, w := range want {
			got, err := tx.Get(k)
			if w == "<absent>" {
				if !errors.Is(err, ErrNotFound) {
					t.Fatalf("视图 v%d 上 %s err=%v，期望 ErrNotFound", tx.snapshotVersion(), k, err)
				}
				continue
			}
			if err != nil || string(got) != w {
				t.Fatalf("视图 v%d 上 %s=%q err=%v，期望 %s", tx.snapshotVersion(), k, got, err, w)
			}
		}
	}
	check(v1, map[string]string{"a": "v1-a", "b": "v1-b", "c": "<absent>"})
	check(v3, map[string]string{"a": "v2-a", "b": "<absent>", "c": "<absent>"})
	check(v5, map[string]string{"a": "v5-a", "b": "<absent>", "c": "v4-c"})

	// 视图活着的时候再推进提交并反复回收：边界只能到最老视图 v1 允许的位置。
	for i := 6; i <= 12; i++ {
		if err := db.Put("a", []byte(fmt.Sprintf("v%d-a", i))); err != nil {
			t.Fatal(err)
		}
	}
	st := runGCUntil(t, db, func(s RetentionStatus) bool {
		// 有效目标被 v1 读者钉在 1；允许若干会话推进到 floor=1 即停。
		return s.HistoryFloorVersion >= 1 && s.EffectiveTargetVersion == 1
	}, 60)
	t.Logf("[回收水位] 三视图存活: current=v%d floor=v%d target=v%d effective=v%d oldest=v%d",
		st.CurrentVersion, st.HistoryFloorVersion, st.RetainTargetVersion,
		st.EffectiveTargetVersion, st.OldestReaderVersion)
	if st.EffectiveTargetVersion != 1 || st.HistoryFloorVersion > 1 {
		t.Fatalf("回收越过了最老视图 v1: floor=%d effective=%d",
			st.HistoryFloorVersion, st.EffectiveTargetVersion)
	}

	// 老视图内容在回收后必须原样。
	check(v1, map[string]string{"a": "v1-a", "b": "v1-b"})
	check(v3, map[string]string{"a": "v2-a", "b": "<absent>"})
	check(v5, map[string]string{"a": "v5-a", "c": "v4-c"})
	t.Log("[判定] 回收推进后三个历史视图读到的状态仍与开启时完全一致")

	v1.Rollback()
	v3.Rollback()
	v5.Rollback()
}

// TestHistoryViewErrorClassification 验证三类失败稳定可区分：
// 版本非法 / 超出保留范围 / 键不存在。
func TestHistoryViewErrorClassification(t *testing.T) {
	opts := retentionOptions(2, 2)
	dir := filepath.Join(t.TempDir(), "db")
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for i := 1; i <= 8; i++ {
		if err := db.Put("k", []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
	}

	// 非法版本：0 与未来版本。
	if _, err := db.BeginReadOnlyAt(0); !errors.Is(err, ErrInvalidVersion) ||
		!errors.Is(err, ErrHistoryNotAvailable) {
		t.Fatalf("at(0) err=%v，期望同时命中 ErrInvalidVersion/ErrHistoryNotAvailable", err)
	}
	if _, err := db.BeginReadOnlyAt(99); !errors.Is(err, ErrInvalidVersion) {
		t.Fatalf("at(99) err=%v，期望 ErrInvalidVersion", err)
	}
	if _, err := db.BeginReadOnlyBack(-1); !errors.Is(err, ErrInvalidVersion) {
		t.Fatalf("back(-1) err=%v，期望 ErrInvalidVersion", err)
	}
	if _, err := db.BeginReadOnlyBack(8); !errors.Is(err, ErrInvalidVersion) {
		t.Fatalf("back(8) err=%v，期望 ErrInvalidVersion（只有 8 个提交）", err)
	}

	// 推进回收使边界到 v6（retain=2, current=8 → 目标 6）。
	st := runGCUntil(t, db, func(s RetentionStatus) bool {
		return s.HistoryFloorVersion >= 6
	}, 60)
	t.Logf("[回收水位] current=v%d floor=v%d target=v%d",
		st.CurrentVersion, st.HistoryFloorVersion, st.RetainTargetVersion)

	_, err = db.BeginReadOnlyAt(5)
	if !errors.Is(err, ErrHistoryNotAvailable) || errors.Is(err, ErrNotFound) {
		t.Fatalf("at(5) err=%v，期望 ErrHistoryNotAvailable 且不是 ErrNotFound", err)
	}
	var he *HistoryError
	if !errors.As(err, &he) {
		t.Fatalf("期望 *HistoryError，实际 %T", err)
	}
	t.Logf("[判定] 越界版本错误携带边界: request=v%d floor=v%d current=v%d",
		he.Version, he.Floor, he.Current)
	if he.Floor != st.HistoryFloorVersion || he.Current != st.CurrentVersion {
		t.Fatalf("HistoryError 边界字段错误: %+v", he)
	}

	// 边界内：v6 可开；“键不存在”必须仍是 ErrNotFound，不与历史错误混淆。
	v6, err := db.BeginReadOnlyAt(6)
	if err != nil {
		t.Fatalf("at(6) 应在保留范围内: %v", err)
	}
	if _, err := v6.Get("never-existed"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("不存在的键 err=%v，期望 ErrNotFound", err)
	}
	v6.Rollback()

	// Back 换算到越界版本同样是 ErrHistoryNotAvailable。
	if _, err := db.BeginReadOnlyBack(7); !errors.Is(err, ErrHistoryNotAvailable) {
		// current=8, back(7)=v1 < floor(6)
		t.Fatalf("back(7) err=%v，期望 ErrHistoryNotAvailable", err)
	}
}

// snapshotKeys 转储给定引擎在若干关注键上的当前态（用于恢复前后对比）。
func snapshotKeys(t *testing.T, db *DB, keys []string, tag string) map[string]string {
	t.Helper()
	out := make(map[string]string, len(keys))
	for _, k := range keys {
		v, err := db.Get(k)
		if errors.Is(err, ErrNotFound) {
			out[k] = "<absent>"
			continue
		}
		if err != nil {
			t.Fatalf("%s: %v", tag, err)
		}
		out[k] = string(v)
	}
	t.Logf("[数据-%s] %v", tag, out)
	return out
}

// TestRetentionBoundaryAndRestart 覆盖保留边界语义、回收多轮推进、
// 以及重启后“已提交一条不少 + 保留边界与回收进度接续”。
func TestRetentionBoundaryAndRestart(t *testing.T) {
	opts := retentionOptions(3, 2)
	dir := filepath.Join(t.TempDir(), "db")

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 12; i++ {
		if err := db.Put(fmt.Sprintf("key-%02d", i), []byte(fmt.Sprintf("val-%02d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Delete("key-04"); err != nil { // v13: 墓碑
		t.Fatal(err)
	}
	keys := make([]string, 0, 12)
	for i := 1; i <= 12; i++ {
		keys = append(keys, fmt.Sprintf("key-%02d", i))
	}
	before := snapshotKeys(t, db, keys, "回收前/崩溃前")

	// current=13, retain=3 → 首个目标 v10。
	st := runGCUntil(t, db, func(s RetentionStatus) bool {
		return s.HistoryFloorVersion >= 10
	}, 100)
	t.Logf("[回收水位] current=v%d floor=v%d retainTarget=v%d effective=v%d 段=%v",
		st.CurrentVersion, st.HistoryFloorVersion, st.RetainTargetVersion,
		st.EffectiveTargetVersion, listSegs(t, dir))
	if st.HistoryFloorVersion != 10 || st.RetainTargetVersion != 10 {
		t.Fatalf("边界错误: floor=%d target=%d", st.HistoryFloorVersion, st.RetainTargetVersion)
	}

	// 边界内的历史视图：v10 上 key-04 已删除（v13 在 v10 之后？否——
	// key-04 删除发生在 v13，所以 v10 视图下它仍然存在）。
	v10, err := db.BeginReadOnlyAt(10)
	if err != nil {
		t.Fatal(err)
	}
	if v, err := v10.Get("key-04"); err != nil || string(v) != "val-04" {
		t.Fatalf("v10 视图 key-04=%q err=%v，期望删除发生前仍可见", v, err)
	}
	v10.Rollback()
	// v13 视图下 key-04 必须是不存在。
	v13, err := db.BeginReadOnlyAt(13)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v13.Get("key-04"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("v13 视图 key-04 err=%v，期望 ErrNotFound", err)
	}
	v13.Rollback()

	// 再推进若干提交，回收边界应继续前移而不是停在 v10。
	for i := 14; i <= 20; i++ {
		if err := db.Put(fmt.Sprintf("key-%02d", i), []byte(fmt.Sprintf("val-%02d", i))); err != nil {
			t.Fatal(err)
		}
	}
	st = runGCUntil(t, db, func(s RetentionStatus) bool {
		return s.HistoryFloorVersion >= 17
	}, 100)
	t.Logf("[回收水位] 继续推进后 current=v%d floor=v%d target=v%d",
		st.CurrentVersion, st.HistoryFloorVersion, st.RetainTargetVersion)
	if st.HistoryFloorVersion != 17 {
		t.Fatalf("边界未继续推进: floor=%d want 17", st.HistoryFloorVersion)
	}
	for i := 14; i <= 20; i++ {
		keys = append(keys, fmt.Sprintf("key-%02d", i))
	}
	before = snapshotKeys(t, db, keys, "崩溃前(含v20)")

	// 干净关闭并重启：数据一条不少，边界接续。
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db2, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("重启失败: %v", err)
	}
	defer db2.Close()
	after := snapshotKeys(t, db2, keys, "重启恢复后")
	for k, w := range before {
		if after[k] != w {
			t.Fatalf("恢复前后不一致 %s: before=%s after=%s", k, w, after[k])
		}
	}
	st2 := db2.RetentionStatus()
	t.Logf("[回收水位] 重启后 current=v%d floor=v%d target=v%d sessionInProgress=%v checkpointFound=%v",
		db2.Version(), st2.HistoryFloorVersion, st2.RetainTargetVersion,
		st2.SessionInProgress, db2.RecoveryStats().CheckpointFound)
	if db2.Version() != 20 || st2.HistoryFloorVersion != 17 {
		t.Fatalf("重启后版本/边界接续错误: version=%d floor=%d",
			db2.Version(), st2.HistoryFloorVersion)
	}
	// 重启后可继续提交，版本单调连续；回收可继续推进。
	if err := db2.Put("key-21", []byte("val-21")); err != nil {
		t.Fatal(err)
	}
	if db2.Version() != 21 {
		t.Fatalf("新版本号不连续: %d", db2.Version())
	}
	st2 = runGCUntil(t, db2, func(s RetentionStatus) bool {
		return s.HistoryFloorVersion >= 18
	}, 100)
	t.Logf("[回收水位] 重启后继续推进 floor=v%d target=v%d",
		st2.HistoryFloorVersion, st2.RetainTargetVersion)
	if st2.HistoryFloorVersion < 18 {
		t.Fatalf("重启后回收无法继续: floor=%d", st2.HistoryFloorVersion)
	}
}

// TestRetentionBoundedRounds 单轮工作量受 MaxKeys/MaxBytes 与 MaxRounds 限制：
// 一次调用不可能把大状态一把写完，ChunksWritten/KeysWritten 有界。
func TestRetentionBoundedRounds(t *testing.T) {
	opts := retentionOptions(1, 3)
	opts.MaxGCPerRoundBytes = 1 << 30 // 只由键数限制
	dir := filepath.Join(t.TempDir(), "db")
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < 30; i++ {
		if err := db.Put(fmt.Sprintf("k%02d", i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	// 每次调用只允许 1 个小轮：第一轮开新会话（首片 + 至多 3 个键）。
	res, err := db.GarbageCollect(GCOptions{MaxRounds: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("[输入/结果] 单轮调用: rounds=%d chunks=%d keys=%d completed=%v",
		res.Rounds, res.ChunksWritten, res.KeysWritten, res.Completed)
	if res.Rounds != 1 || res.Completed || res.KeysWritten > 3 {
		t.Fatalf("单轮工作量越界: %+v", res)
	}
	st := db.RetentionStatus()
	if !st.SessionInProgress {
		t.Fatal("单轮后应当存在进行中的检查点会话")
	}
	t.Logf("[回收水位] 会话进行中: version=v%d keys=%d/%d chunks=%d",
		st.SessionVersion, st.SessionKeysDone, st.SessionKeysTotal, st.SessionChunksWritten)
}

// TestAutoGCAdvancesInBackground 启用 EnableAutoGC 后，不做任何显式 GC，
// 仅持续提交，后台回收应把保留边界自动向前推进；提交路径仍能正常成功。
func TestAutoGCAdvancesInBackground(t *testing.T) {
	opts := retentionOptions(3, 2)
	opts.EnableAutoGC = true
	dir := filepath.Join(t.TempDir(), "db")
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < 60; i++ {
		if err := db.Put(fmt.Sprintf("k%02d", i%20), []byte("v")); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Millisecond) // 给后台小步回收留时间
	}

	// 等待后台推进：current=60, retain=3 → 目标至少应远离 0。
	deadline := time.Now().Add(3 * time.Second)
	var st RetentionStatus
	for time.Now().Before(deadline) {
		st = db.RetentionStatus()
		if st.HistoryFloorVersion > 0 && st.HistoryFloorVersion >= st.RetainTargetVersion-1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Logf("[回收水位] 自动回收后 current=v%d floor=v%d target=v%d oldest=v%d 段=%d",
		st.CurrentVersion, st.HistoryFloorVersion, st.RetainTargetVersion,
		st.OldestReaderVersion, len(listSegs(t, dir)))
	if st.HistoryFloorVersion == 0 {
		t.Fatal("后台回收应已产生检查点并推进边界")
	}
	if st.HistoryFloorVersion < st.RetainTargetVersion-1 {
		t.Fatalf("后台回收推进不足: floor=%d target=%d",
			st.HistoryFloorVersion, st.RetainTargetVersion)
	}
	// 边界外历史视图应被拒绝，边界内可用。
	if _, err := db.BeginReadOnlyAt(1); !errors.Is(err, ErrHistoryNotAvailable) {
		t.Fatalf("越界视图应被拒绝: %v", err)
	}
	if _, err := db.BeginReadOnlyAt(st.HistoryFloorVersion); err != nil {
		t.Fatalf("边界版本视图应可用: %v", err)
	}
}

// TestDefaultOptionsUnchangedCompatibility 默认配置（RetainVersions=0）下
// 新增 API 与字段必须不改变旧行为：GarbageCollect 仍走整段检查点路径，
// 历史视图在无检查点时按“从未保留”处理（floor=0 时所有版本理论可见）。
func TestDefaultOptionsUnchangedCompatibility(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	db, err := Open(dir, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < 6; i++ {
		if err := db.Put("k", []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	st := db.RetentionStatus()
	if st.Enabled {
		t.Fatal("默认配置不应启用保留范围")
	}
	if st.HistoryFloorVersion != 0 || st.RetainTargetVersion != 0 {
		t.Fatalf("默认配置水位应为 0: %+v", st)
	}

	// 旧 GC 行为：无老读者时整段检查点成功（走非增量路径）。
	res, err := db.GarbageCollect(GCOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Incremental || res.CheckpointSegSeq == 0 {
		t.Fatalf("默认配置应走旧整段检查点路径: %+v", res)
	}
	t.Logf("[兼容] 旧 GC 路径检查点 v%d 所在段=%d 删除旧段=%d",
		res.CheckpointVer, res.CheckpointSegSeq, res.SegmentsRemoved)

	// floor=0 的兼容目录：历史视图仍以旧检查点为基线可开（旧目录有整段 cp）。
	tx, err := db.BeginReadOnlyAt(res.CheckpointVer)
	if err != nil {
		t.Fatalf("旧模式检查点版本应可开视图: %v", err)
	}
	tx.Rollback()
}

// TestIncrementalGCReaderDoesNotBlockForever 增量模式下：
// 老读者存活期间，回收能收到“被读者钉住”的位置但不越过它，也不整体拒绝；
// 读者结束后回收自动继续推进，最终追平保留目标。
func TestIncrementalGCReaderDoesNotBlockForever(t *testing.T) {
	opts := retentionOptions(2, 2)
	dir := filepath.Join(t.TempDir(), "db")
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for i := 1; i <= 10; i++ {
		if err := db.Put(fmt.Sprintf("k%02d", i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	// 读者钉在 v5：current=10, retain=2 → 目标 v8，但有效目标被压到 v5。
	reader, err := db.BeginReadOnlyAt(5)
	if err != nil {
		t.Fatal(err)
	}
	st := runGCUntil(t, db, func(s RetentionStatus) bool {
		return s.HistoryFloorVersion >= 5
	}, 100)
	t.Logf("[回收水位] 读者存活: floor=v%d effective=v%d oldest=v%d session=%v",
		st.HistoryFloorVersion, st.EffectiveTargetVersion,
		st.OldestReaderVersion, st.SessionInProgress)
	if st.HistoryFloorVersion != 5 {
		t.Fatalf("边界应停在读者快照 v5，实际 v%d", st.HistoryFloorVersion)
	}
	// v5 视图数据仍在。
	if _, err := reader.Get("k05"); err != nil {
		t.Fatalf("读者视图被破坏: %v", err)
	}
	// 此时不应能推进到 v8（段也不应误删读者所需历史）。
	st = runGCUntil(t, db, func(s RetentionStatus) bool { return true }, 5)
	if st.HistoryFloorVersion > 5 {
		t.Fatalf("读者存活期间边界被越过: v%d", st.HistoryFloorVersion)
	}
	reader.Rollback()

	// 读者离开后：显式 GC 追到目标 v8。
	st = runGCUntil(t, db, func(s RetentionStatus) bool {
		return s.HistoryFloorVersion >= 8
	}, 100)
	t.Logf("[回收水位] 读者离开: floor=v%d target=v%d",
		st.HistoryFloorVersion, st.RetainTargetVersion)
	if st.HistoryFloorVersion < 8 {
		t.Fatalf("读者离开后回收未自动续收到 v8: floor=v%d", st.HistoryFloorVersion)
	}
}
