package storage

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

// tinySegmentOptions 段小到大约一条提交记录就滚动一段，
// 让增量回收可以按“段”粒度逐段推进（段内版本全部 <= 水位才可删）。
func tinySegmentOptions() Options {
	o := smallSegmentOptions()
	o.MaxWalSegmentBytes = 40
	return o
}

// commitSeq 顺序提交 n 个版本，每个版本把 key 写成 fmt.Sprintf(format, i)，
// 返回最后一个版本号。起始版本为 start。
func commitSeq(t *testing.T, db *DB, key, format string, start, n int) uint64 {
	t.Helper()
	for i := start; i < start+n; i++ {
		if err := db.Put(key, []byte(fmt.Sprintf(format, i))); err != nil {
			t.Fatal(err)
		}
	}
	return db.Version()
}

// TestHistoricalViewAtCommitPoint 覆盖：按提交点开历史视图，
// 视图读到那一刻已提交的完整状态；之后的提交/删除不影响它；
// 删除过的键在视图中表现为不存在。
func TestHistoricalViewAtCommitPoint(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	db, err := Open(dir, smallSegmentOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// v1: a=1,b=1  v2: a=2  v3: del b  v4: c=4
	tx, _ := db.Begin()
	_ = tx.Put("a", []byte("a1"))
	_ = tx.Put("b", []byte("b1"))
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := db.Put("a", []byte("a2")); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete("b"); err != nil {
		t.Fatal(err)
	}
	if err := db.Put("c", []byte("c4")); err != nil {
		t.Fatal(err)
	}
	t.Logf("[输入] 已提交 v1..v%d: v1{a=a1,b=b1} v2{a=a2} v3{del b} v4{c=c4}", db.Version())

	// 从 v2 开历史视图：应看到 a=a2、b=b1，没有 c。
	view, err := db.BeginReadOnlyAt(2)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("[输入] 从提交点 v2 开启历史视图")

	// 视图开启后再提交 v5：a=a5，不能影响视图。
	if err := db.Put("a", []byte("a5")); err != nil {
		t.Fatal(err)
	}
	t.Logf("[输入] 视图开启后又提交 v%d: a=a5", db.Version())

	checks := []struct {
		key, want string
	}{
		{"a", "a2"}, {"b", "b1"},
	}
	for _, c := range checks {
		got, err := view.Get(c.key)
		if err != nil || string(got) != c.want {
			t.Fatalf("[判定] 历史视图 v2 中 %s=%q err=%v，期望 %q", c.key, got, err, c.want)
		}
		t.Logf("[判定] 历史视图 v2 中 %s=%s（之后提交的 a5 不可见）", c.key, got)
	}
	// v2 时 c 尚未提交 → 不存在。
	if _, err := view.Get("c"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("[判定] 历史视图 v2 中 c 应不存在，实际 err=%v", err)
	}
	t.Log("[判定] 历史视图 v2 中 c 不存在（v4 才提交）")

	// 从 v3 开视图：b 已被删除 → 表现为不存在。
	view3, err := db.BeginReadOnlyAt(3)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := view3.Get("b"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("[判定] 历史视图 v3 中已删除的 b 应表现为不存在，实际 err=%v", err)
	}
	t.Log("[判定] 历史视图 v3 中已删除的 b 表现为 ErrNotFound")
	view3.Rollback()
	view.Rollback()

	// 结束后的视图再读 → 事务已失效（与键不存在、历史过期是不同类别）。
	if _, err := view.Get("a"); !errors.Is(err, ErrTxnNotActive) {
		t.Fatalf("[判定] 视图结束后读取应返回 ErrTxnNotActive，实际 %v", err)
	}
	t.Log("[判定] 视图结束后读取返回 ErrTxnNotActive")
}

// TestBeginReadOnlyLastN 覆盖“往前回看最近第 N 次提交”的省事说法及边界。
func TestBeginReadOnlyLastN(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	db, err := Open(dir, smallSegmentOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	commitSeq(t, db, "k", "v%d", 1, 5) // v1..v5: k=v1..v5
	t.Logf("[输入] 已提交 5 个版本，最新版本 v%d", db.Version())

	// 最近第 1 次提交 = 最新版本 v5。
	v, err := db.BeginReadOnlyLastN(1)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := v.Get("k"); string(got) != "v5" {
		t.Fatalf("[判定] LastN(1) 应看到 v5，实际 %s", got)
	}
	t.Logf("[判定] LastN(1) 视图快照=v%d，k=v5", v.snapshotVersion())
	v.Rollback()

	// 最近第 3 次提交 = v3。
	v, err = db.BeginReadOnlyLastN(3)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := v.Get("k"); string(got) != "v3" {
		t.Fatalf("[判定] LastN(3) 应看到 v3，实际 %s", got)
	}
	t.Logf("[判定] LastN(3) 视图快照=v%d，k=v3", v.snapshotVersion())
	v.Rollback()

	// n=0 指向“最新之后”的提交点 → ErrSnapshotInFuture。
	_, err = db.BeginReadOnlyLastN(0)
	if !errors.Is(err, ErrSnapshotInFuture) {
		t.Fatalf("[判定] LastN(0) 应返回 ErrSnapshotInFuture，实际 %v", err)
	}
	t.Logf("[判定] LastN(0) 返回 ErrSnapshotInFuture: %v", err)

	// n 超过已有提交数太多 → 回看点在保留范围之前 → ErrSnapshotExpired。
	_, err = db.BeginReadOnlyLastN(100)
	if !errors.Is(err, ErrSnapshotExpired) {
		t.Fatalf("[判定] LastN(100) 应返回 ErrSnapshotExpired，实际 %v", err)
	}
	t.Logf("[判定] LastN(100) 返回 ErrSnapshotExpired: %v", err)
}

// TestSnapshotExpiredDistinctErrors 覆盖：历史过期 / 键不存在 / 事务失效
// 三类原因必须能用 errors.Is 稳定区分。
func TestSnapshotExpiredDistinctErrors(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	// 一段约一条提交，保证回收一定能推进到保留窗口下沿（不受段内版本混杂影响）。
	opts := tinySegmentOptions()
	opts.RetainVersions = 3
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	commitSeq(t, db, "k", "v%d", 1, 10) // v1..v10
	// 手动推进回收，直到历史边界到达保留窗口下沿。
	for {
		res, err := db.ReclaimLog()
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("[回收] 本轮水位=v%d 边界 %d→%d 删段=%d skipped=%v pending=%v",
			res.Horizon, res.FloorBefore, res.FloorAfter, res.SegmentsRemoved, res.Skipped, res.Pending)
		if res.Skipped {
			break
		}
	}
	info := db.RetentionInfo()
	t.Logf("[输入] 保留窗口=最近 %d 版，当前历史边界=v%d，最新版本=v%d",
		info.RetainVersions, info.HistoryFloor, info.CurrentVersion)
	if info.HistoryFloor != 8 {
		t.Fatalf("前置条件：历史边界应推进到 v8（10-3+1），实际 v%d", info.HistoryFloor)
	}

	// 1) 落在保留范围之外的提交点 → ErrSnapshotExpired。
	_, err = db.BeginReadOnlyAt(5)
	if !errors.Is(err, ErrSnapshotExpired) {
		t.Fatalf("[判定] v5 已出保留范围，应返回 ErrSnapshotExpired，实际 %v", err)
	}
	if errors.Is(err, ErrNotFound) || errors.Is(err, ErrTxnNotActive) {
		t.Fatalf("[判定] ErrSnapshotExpired 不得与 ErrNotFound/ErrTxnNotActive 混淆: %v", err)
	}
	var se *SnapshotError
	if errors.As(err, &se) {
		t.Logf("[判定] 过期错误携带细节: 请求 v%d，保留自 v%d，最新 v%d", se.Want, se.Floor, se.Current)
	}

	// 2) 保留范围内的视图里，已删除/不存在的键 → ErrNotFound。
	view, err := db.BeginReadOnlyAt(8)
	if err != nil {
		t.Fatalf("[判定] v8 在保留范围内，应能开启视图: %v", err)
	}
	if _, err := view.Get("never-existed"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("[判定] 视图内不存在的键应返回 ErrNotFound，实际 %v", err)
	}
	t.Log("[判定] 视图内不存在的键返回 ErrNotFound（与过期不同类别）")

	// 3) 视图结束后 → ErrTxnNotActive。
	view.Rollback()
	if _, err := view.Get("k"); !errors.Is(err, ErrTxnNotActive) {
		t.Fatalf("[判定] 视图结束后应返回 ErrTxnNotActive，实际 %v", err)
	}
	t.Log("[判定] 结束后的视图返回 ErrTxnNotActive（三类错误两两可区分）")

	// 4) 未来版本 → ErrSnapshotInFuture。
	if _, err := db.BeginReadOnlyAt(99); !errors.Is(err, ErrSnapshotInFuture) {
		t.Fatalf("[判定] 未来版本应返回 ErrSnapshotInFuture，实际 %v", err)
	}
	t.Log("[判定] 未来版本返回 ErrSnapshotInFuture")
}

// TestMultipleHistoricalViewsConcurrently 覆盖多个历史视图同时存在：
// 各自读到一致的快照；回收水位被最老视图钳制，视图逐个释放后逐段推进。
// 本用例不启用保留窗口（RetainVersions=0，手动驱动回收），
// 避免后台回收与断言时序交织；自动推进由 reclaim_test.go 覆盖。
func TestMultipleHistoricalViewsConcurrently(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	opts := tinySegmentOptions() // 一段约一条提交，回收可按段逐段推进
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	commitSeq(t, db, "k", "v%d", 1, 10) // v1..v10
	t.Logf("[输入] 已提交 v1..v%d，手动驱动回收（目标水位=最新版本）", db.Version())

	// 同时开三个历史视图：v4、v6、v8。
	v4, err := db.BeginReadOnlyAt(4)
	if err != nil {
		t.Fatal(err)
	}
	v6, err := db.BeginReadOnlyAt(6)
	if err != nil {
		t.Fatal(err)
	}
	v8, err := db.BeginReadOnlyAt(8)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("[输入] 同时开启 v4/v6/v8 三个历史视图")

	// 并发读：每个视图必须始终读到自己快照的值（race 检测下验证无视图间干扰）。
	var wg sync.WaitGroup
	for _, tc := range []struct {
		view *Txn
		want string
	}{{v4, "v4"}, {v6, "v6"}, {v8, "v8"}} {
		wg.Add(1)
		go func(view *Txn, want string) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				got, err := view.Get("k")
				if err != nil || string(got) != want {
					t.Errorf("视图应恒读到 %s，实际 %q err=%v", want, got, err)
					return
				}
			}
		}(tc.view, tc.want)
	}
	wg.Wait()
	t.Log("[判定] 三个视图并发各读 50 次，均恒为各自快照值")

	// 回收一轮：水位必须被最老视图 v4 钳制，而不是整体拒绝。
	res, err := db.ReclaimLog()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("[回收] 有 v4/v6/v8 视图时：水位=v%d 边界→v%d 被读者钳制=v%d pending=%v",
		res.Horizon, res.FloorAfter, res.BlockedByReader, res.Pending)
	if res.FloorAfter != 4 || res.BlockedByReader != 4 {
		t.Fatalf("[判定] 回收应推进到最老视图 v4 并被其钳制，实际边界=v%d 钳制=v%d",
			res.FloorAfter, res.BlockedByReader)
	}
	if !res.Pending {
		t.Fatal("[判定] 窗口外仍有历史，应标记 Pending")
	}
	// v3 已出保留范围。
	if _, err := db.BeginReadOnlyAt(3); !errors.Is(err, ErrSnapshotExpired) {
		t.Fatalf("[判定] 边界推进到 v4 后，v3 视图应过期，实际 %v", err)
	}

	// 释放 v4 → 推进到 v6；释放 v6 → 推进到 v8。
	v4.Rollback()
	res, _ = db.ReclaimLog()
	t.Logf("[回收] 释放 v4 后：边界→v%d（被 v6 钳制=%d）", res.FloorAfter, res.BlockedByReader)
	if res.FloorAfter != 6 {
		t.Fatalf("[判定] 释放 v4 后边界应推进到 v6，实际 v%d", res.FloorAfter)
	}
	v6.Rollback()
	res, _ = db.ReclaimLog()
	t.Logf("[回收] 释放 v6 后：边界→v%d（被 v8 钳制=%d）", res.FloorAfter, res.BlockedByReader)
	if res.FloorAfter != 8 {
		t.Fatalf("[判定] 释放 v6 后边界应推进到 v8，实际 v%d", res.FloorAfter)
	}

	// 还活着的 v8 视图依然能正确读。
	if got, _ := v8.Get("k"); string(got) != "v8" {
		t.Fatalf("[判定] v8 视图在回收推进后仍应读到 v8，实际 %s", got)
	}
	t.Log("[判定] 回收逐段推进期间，存活视图读取不受影响")

	// 全部释放后：回收到最新版本 v10（RetainVersions=0 时目标水位=最新）。
	v8.Rollback()
	for {
		res, _ = db.ReclaimLog()
		if res.Skipped {
			break
		}
	}
	info := db.RetentionInfo()
	t.Logf("[回收] 全部视图释放后：边界=v%d pending=%v", info.HistoryFloor, info.PendingReclaim)
	if info.HistoryFloor != 10 {
		t.Fatalf("[判定] 最终边界应推进到 v10，实际 v%d", info.HistoryFloor)
	}
}
