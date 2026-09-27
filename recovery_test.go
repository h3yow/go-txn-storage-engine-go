package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// dumpState 把引擎当前全部关注的键打印出来，作为恢复前后的对比依据。
func dumpState(t *testing.T, db *DB, keys []string, tag string) map[string]string {
	t.Helper()
	out := make(map[string]string)
	for _, k := range keys {
		v, err := db.Get(k)
		if errors.Is(err, ErrNotFound) {
			out[k] = "<deleted/absent>"
			continue
		}
		if err != nil {
			t.Fatalf("%s get %s: %v", tag, k, err)
		}
		out[k] = string(v)
	}
	t.Logf("[数据-%s] %v", tag, out)
	return out
}

// appendGarbage 在日志末尾直接拼接“半条记录”，模拟提交写盘到一半被强杀。
func appendGarbage(t *testing.T, path string, garbage []byte) int64 {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	n, err := f.Write(garbage)
	if err != nil {
		t.Fatal(err)
	}
	return int64(n)
}

// TestRecoverTornTail 覆盖需求场景：
// 已提交事务 fsync 完成；随后进程在写下一条提交记录的中途被强杀，
// 日志末尾留下半帧。重启时引擎必须认出并截断残尾、照常启动，
// 已提交数据完整可见，“半个版本”绝不能出现。
func TestRecoverTornTail(t *testing.T) {
	opts := DefaultOptions()
	dir := filepath.Join(t.TempDir(), "db")
	keys := []string{"committed-1", "committed-2", "uncommitted"}

	// 阶段一：两个完整提交。
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put("committed-1", []byte("C1")); err != nil {
		t.Fatal(err)
	}
	if err := db.Put("committed-2", []byte("C2")); err != nil {
		t.Fatal(err)
	}
	verBefore := db.Version()
	before := dumpState(t, db, keys, "崩溃前")
	t.Logf("[输入] 两个事务已提交并 fsync，最新版本 v%d", verBefore)

	// 阶段二：构造“正在提交但只写了半个帧”的崩溃现场。
	// 真实代码路径不会追加半截帧，这里手工编码一个完整提交帧再截短，
	// 使残片具有合法 magic + 长度字段，但 payload/CRC 不完整。
	fullFrame := encodeFrame(encodeCommit(commitRec{
		version: verBefore + 1,
		writes:  []writeEntry{{key: "uncommitted", value: []byte("PARTIAL")}},
	}))
	// 先拿到活动段路径。
	segs, _ := listWalSegments(dir)
	activeSeg := filepath.Join(dir, segs[len(segs)-1])
	tornLen := int64(walHdrSize + 7) // 只有帧头和 7 字节 payload
	t.Logf("[输入] 模拟强杀：向 %s 尾部写入半帧 %d/%d 字节（magic+长度合法，payload 不全）",
		filepath.Base(activeSeg), tornLen, len(fullFrame))
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if n := appendGarbage(t, activeSeg, fullFrame[:tornLen]); n != tornLen {
		t.Fatalf("garbage write len=%d", n)
	}

	// 阶段三：重启恢复。引擎应识别并截断残尾，而不是启动失败。
	db2, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("[判定] 尾部残帧不应导致启动失败: %v", err)
	}
	stats := db2.RecoveryStats()
	t.Logf("[判定] 恢复统计: 扫描段=%d 截断段=%s 截断字节=%d 提交记录=%d 最新版本=v%d",
		stats.SegmentsScanned, stats.TruncatedSegment, stats.TruncatedBytes,
		stats.CommitRecords, db2.Version())
	if stats.TruncatedSegment == "" {
		t.Fatal("期望报告被截断的残尾段")
	}
	if stats.TruncatedBytes != tornLen {
		t.Fatalf("截断字节=%d，期望 %d", stats.TruncatedBytes, tornLen)
	}
	if db2.Version() != verBefore {
		t.Fatalf("恢复后版本=v%d，期望 v%d（半截提交不得成为版本）", db2.Version(), verBefore)
	}

	after := dumpState(t, db2, keys, "重启恢复后")
	for k, v := range before {
		if after[k] != v {
			t.Fatalf("键 %s 恢复前后不一致: before=%q after=%q", k, v, after[k])
		}
	}
	if after["uncommitted"] != "<deleted/absent>" {
		t.Fatalf("半截提交的数据不得出现: uncommitted=%q", after["uncommitted"])
	}

	// 阶段四：被截断的段继续追加仍正常，且新版本号连续。
	if err := db2.Put("after-restart", []byte("OK")); err != nil {
		t.Fatal(err)
	}
	v, _ := db2.Get("after-restart")
	if string(v) != "OK" || db2.Version() != verBefore+1 {
		t.Fatalf("恢复后写入异常: v=%s version=v%d", v, db2.Version())
	}
	t.Logf("[结果] 残尾截断后新提交成功 after-restart=%s，版本连续到 v%d", v, db2.Version())

	// 再重启一次：上一阶段尾部已经是干净帧，不应再有截断。
	if err := db2.Close(); err != nil {
		t.Fatal(err)
	}
	db3, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db3.Close()
	st3 := db3.RecoveryStats()
	if st3.TruncatedSegment != "" {
		t.Fatalf("干净日志二次重启不应截断，实际 %s", st3.TruncatedSegment)
	}
	v, _ = db3.Get("after-restart")
	if string(v) != "OK" {
		t.Fatalf("二次重启数据丢失: after-restart=%s", v)
	}
	t.Logf("[结果] 二次重启无残尾截断，数据依旧完整")
}

// TestRecoverTornTailCRCMismatch 覆盖另一种半截形态：帧长度完整但 CRC 不符
// （页缓存部分写穿等极端情况），同样按残尾跳过。
func TestRecoverTornTailCRCMismatch(t *testing.T) {
	opts := DefaultOptions()
	dir := filepath.Join(t.TempDir(), "db")
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put("good", []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	segs, _ := listWalSegments(dir)
	seg := filepath.Join(dir, segs[len(segs)-1])

	// 手工构造一个“长度完整但 CRC 是错的”帧。
	payload := []byte{recTypeCommit, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0}
	frame := encodeFrame(payload)
	binary.BigEndian.PutUint32(frame[len(frame)-4:], 0xDEADBEEF) // 故意写坏 CRC
	appendGarbage(t, seg, frame)

	db2, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("CRC 残尾也应被跳过: %v", err)
	}
	defer db2.Close()
	st := db2.RecoveryStats()
	if st.TruncatedSegment == "" || st.TruncatedBytes != int64(len(frame)) {
		t.Fatalf("CRC 残尾截断异常: seg=%s bytes=%d frameLen=%d",
			st.TruncatedSegment, st.TruncatedBytes, len(frame))
	}
	v, _ := db2.Get("good")
	if string(v) != "1" {
		t.Fatalf("good=%s 期望 1", v)
	}
	t.Logf("[判定] CRC 不符的末尾帧按残尾截断（%d 字节），已提交数据 good=1 完整", len(frame))
}

// TestCorruptionMidLogRejected 残尾容忍不能掩盖真正的中段损坏：
// 坏帧之后如果还有完整帧，引擎必须拒绝启动并给出 ErrLogCorrupted。
func TestCorruptionMidLogRejected(t *testing.T) {
	opts := smallSegmentOptions()
	dir := filepath.Join(t.TempDir(), "db")
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	// 写够多的数据以产生 >=2 个段。
	for i := 0; i < 24; i++ {
		if err := db.Put(fmt.Sprintf("k%d", i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	segs, _ := listWalSegments(dir)
	if len(segs) < 2 {
		t.Fatal("前置条件：应产生多段")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// 破坏第一个段中段的一个字节（magic 区域之外仍属某帧的一部分）。
	first := filepath.Join(dir, segs[0])
	f, err := os.OpenFile(first, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	fi, _ := f.Stat()
	off := fi.Size() / 2
	if _, err := f.WriteAt([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}, off); err != nil {
		t.Fatal(err)
	}
	f.Close()

	_, err = Open(dir, opts)
	if !errors.Is(err, ErrLogCorrupted) {
		t.Fatalf("[判定] 中段损坏应返回 ErrLogCorrupted，实际 err=%v", err)
	}
	t.Logf("[判定] 非尾部损坏被识别，引擎拒绝启动: %v", err)
}

// TestUncommittedLeavesNothing 回滚与提交失败不得留下任何版本。
func TestUncommittedLeavesNothing(t *testing.T) {
	db, _ := newTestDB(t, DefaultOptions())
	defer db.Close()

	t1, _ := db.Begin()
	if err := t1.Put("phantom", []byte("x")); err != nil {
		t.Fatal(err)
	}
	t1.Rollback()
	if _, err := db.Get("phantom"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("回滚后 phantom err=%v，期望 ErrNotFound", err)
	}

	// 写写冲突失败的一方数据同样不得泄漏。
	if err := db.Put("shared", []byte("a")); err != nil {
		t.Fatal(err)
	}
	a, _ := db.Begin()
	b, _ := db.Begin()
	_ = a.Put("shared", []byte("a1"))
	_ = b.Put("shared", []byte("b1"))
	if err := a.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(); !errors.Is(err, ErrWriteWriteConflict) {
		t.Fatalf("b 应冲突，实际 %v", err)
	}
	v, _ := db.Get("shared")
	if string(v) != "a1" {
		t.Fatalf("冲突方写入泄漏: shared=%s，期望 a1", v)
	}
	t.Logf("[判定] 回滚/冲突失败均未留下多余版本，shared=%s", v)
}
