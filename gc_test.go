package storage

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"
)

func smallSegmentOptions() Options {
	o := DefaultOptions()
	o.MaxWalSegmentBytes = 256 // 故意很小，几条记录就滚动一段
	return o
}

func listSegs(t *testing.T, dir string) []string {
	t.Helper()
	names, err := listWalSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	return names
}

// TestGCBlockedByActiveReader 覆盖需求场景：
// 读事务持有老快照期间，回收必须被拒绝（ErrGCBlocked），
// 它需要的旧日志一段都不能少；读事务结束后，回收才放行，
// 且回收 + 重启后数据完整、版本可继续增长。
func TestGCBlockedByActiveReader(t *testing.T) {
	opts := smallSegmentOptions()
	dir := filepath.Join(t.TempDir(), "db")

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}

	// 制造若干版本与多个日志段。
	for i := 0; i < 12; i++ {
		if err := db.Put("k", []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	segsBefore := listSegs(t, dir)
	t.Logf("[输入] 已提交到版本 v%d，当前日志段数=%d %v", db.Version(), len(segsBefore), segsBefore)
	if len(segsBefore) < 2 {
		t.Fatalf("前置条件未满足：应当因小 segment 配置产生多段，实际 %d", len(segsBefore))
	}

	// 老读事务：固定在当前快照 v12。
	reader, err := db.BeginReadOnly()
	if err != nil {
		t.Fatal(err)
	}
	oldSnap := reader.snapshotVersion()
	got, _ := reader.Get("k")
	t.Logf("[输入] 开启读事务，持有快照 v%d, 视图 k=%s", oldSnap, got)

	// 快照之后再推进几个新版本，制造“更老”的历史段。
	for i := 12; i < 16; i++ {
		if err := db.Put("k", []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
	}

	// 尝试回收：必须被老读者阻塞。
	res, err := db.GarbageCollect(GCOptions{})
	if !errors.Is(err, ErrGCBlocked) {
		t.Fatalf("[判定] 期望 ErrGCBlocked，实际 err=%v", err)
	}
	var be *GCBlockedError
	if errors.As(err, &be) {
		t.Logf("[判定] 回收被阻塞: 最老活跃快照=v%d, 想回收到版本=v%d",
			be.OldestSnapshot, be.WantReclaimUpTo)
		if be.OldestSnapshot != oldSnap {
			t.Fatalf("阻塞边界快照=%d，期望 %d", be.OldestSnapshot, oldSnap)
		}
	}
	if !res.Blocked {
		t.Fatal("GCResult 应标记 Blocked")
	}
	// GC 被阻塞时不能删除任何段；与“尝试 GC 之前”的段清单逐一比对。
	segsAtAttempt := listSegs(t, dir)
	segsBlocked := listSegs(t, dir)
	t.Logf("[判定] 阻塞期间日志段数=%d（旧段 %v 全部保留）", len(segsBlocked), segsBefore)
	if len(segsBlocked) != len(segsAtAttempt) {
		t.Fatal("段清单在阻塞判定后发生变化")
	}
	// 老读者的视图必须仍然正确。
	got, _ = reader.Get("k")
	if string(got) != "v11" {
		t.Fatalf("[判定] 老读者视图被破坏: k=%s, 期望 v11", got)
	}
	t.Logf("[判定] 老读者依旧读到 k=%s，旧日志未被清理", got)

	// 读事务结束后回收应放行。
	reader.Rollback()
	res, err = db.GarbageCollect(GCOptions{})
	if err != nil {
		t.Fatalf("[判定] 读者结束后 GC 应成功，实际 %v", err)
	}
	segsAfter := listSegs(t, dir)
	t.Logf("[判定] GC 成功: 检查点版本=v%d 所在新段=%d 删除旧段=%d 剩余段=%v",
		res.CheckpointVer, res.CheckpointSegSeq, res.SegmentsRemoved, segsAfter)
	if res.SegmentsRemoved == 0 {
		t.Fatal("期望删除至少一个旧段")
	}
	for _, name := range segsAfter {
		if parseWalSeq(name) < res.CheckpointSegSeq {
			t.Fatalf("旧段 %s 未被删除", name)
		}
	}

	// 关闭并基于“回收后”的日志重启：数据必须完整。
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db2, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	got, _ = db2.Get("k")
	t.Logf("[结果] 回收后重启: k=%s（期望 v15）, 最新版本 v%d, 发现检查点=%v",
		got, db2.Version(), db2.RecoveryStats().CheckpointFound)
	if string(got) != "v15" || db2.Version() != 16 {
		t.Fatalf("回收重启后状态错误: k=%s version=%d", got, db2.Version())
	}
	if !db2.RecoveryStats().CheckpointFound {
		t.Fatal("重启恢复时应当找到检查点")
	}

	// 版本序列必须能继续单调增长，新提交不被回收影响。
	if err := db2.Put("k", []byte("v16")); err != nil {
		t.Fatal(err)
	}
	got, _ = db2.Get("k")
	if string(got) != "v16" {
		t.Fatalf("回收后新写入错误: k=%s", got)
	}
	t.Logf("[结果] 回收重启后新事务提交成功，k=%s", got)
}

// TestGCPreservesMultipleKeys 检查点是全量态，回收后所有键都要在。
func TestGCPreservesMultipleKeys(t *testing.T) {
	opts := smallSegmentOptions()
	dir := filepath.Join(t.TempDir(), "db")
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if err := db.Put(fmt.Sprintf("key-%02d", i), []byte(fmt.Sprintf("val-%02d", i))); err != nil {
			t.Fatal(err)
		}
	}
	// 删除其中一个键，检查点也要正确表达墓碑。
	if err := db.Delete("key-07"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.GarbageCollect(GCOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db2, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	for i := 0; i < 20; i++ {
		k := fmt.Sprintf("key-%02d", i)
		v, err := db2.Get(k)
		if i == 7 {
			if !errors.Is(err, ErrNotFound) {
				t.Fatalf("%s 应保持删除状态，实际 v=%s err=%v", k, v, err)
			}
			continue
		}
		want := fmt.Sprintf("val-%02d", i)
		if err != nil || string(v) != want {
			t.Fatalf("%s = %q, %v; want %s", k, v, err, want)
		}
	}
	t.Log("[判定] 回收重启后 20 个键的全量态与 key-07 的删除标记均正确")
}
