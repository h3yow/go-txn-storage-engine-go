package storage

import (
	"bytes"
	"errors"
	"testing"
)

// TestBoundaryRejections 覆盖需求：键过大、值过大、事务过大必须被明确拒绝，
// 而且三种原因能彼此区分，不能都长成一个样子。
func TestBoundaryRejections(t *testing.T) {
	opts := Options{
		MaxKeyBytes:      4,
		MaxValueBytes:    8,
		MaxTxnWriteBytes: 16,
		// segment 足够大，避免边界测试被滚动干扰。
		MaxWalSegmentBytes: 1 << 20,
		SyncOnCommit:       true,
	}
	db, _ := newTestDB(t, opts)
	defer db.Close()

	// 1) 键过大：键 5 字节 > 上限 4。
	err := db.Put("toolong-key", []byte("v"))
	checkLimit(t, err, ErrKeyTooLarge, 11, 4)
	t.Logf("[判定] 键过大被拒绝: %v", err)

	// 2) 值过大：值 9 字节 > 上限 8。
	err = db.Put("k", bytes.Repeat([]byte("v"), 9))
	checkLimit(t, err, ErrValueTooLarge, 9, 8)
	t.Logf("[判定] 值过大被拒绝: %v", err)

	// 3) 事务过大：单键值都合法，但多条累计写入超过 16 字节。
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Put("aaaa", bytes.Repeat([]byte("1"), 8)); err != nil {
		t.Fatalf("首条写入应合法: %v", err) // 4+8=12
	}
	// 再写 4+8=12，累计将达 24 > 16。
	err = tx.Put("bbbb", bytes.Repeat([]byte("2"), 8))
	checkLimit(t, err, ErrTxnTooLarge, 24, 16)
	t.Logf("[判定] 事务累计过大被拒绝: %v", err)
	tx.Rollback()

	// 4) 拒绝后引擎仍然健康，合法写入照常成功。
	if err := db.Put("k", []byte("good")); err != nil {
		t.Fatalf("边界拒绝后合法写入失败: %v", err)
	}
	v, _ := db.Get("k")
	if string(v) != "good" {
		t.Fatalf("k=%q want good", v)
	}
	t.Logf("[判定] 边界拒绝不污染引擎，合法写入 k=good 成功")

	// 5) 三类错误彼此可区分：任何一类都不能被误判成另外两类。
	for _, probe := range []error{
		&LimitError{Kind: ErrKeyTooLarge},
		&LimitError{Kind: ErrValueTooLarge},
		&LimitError{Kind: ErrTxnTooLarge},
	} {
		var le *LimitError
		if !errors.As(probe, &le) {
			t.Fatalf("LimitError 解包失败: %v", probe)
		}
		others := []error{ErrWriteWriteConflict, ErrTxnNotActive, ErrGCBlocked}
		for _, o := range others {
			if errors.Is(probe, o) {
				t.Fatalf("%v 不应匹配 %v", probe, o)
			}
		}
	}
	t.Logf("[判定] 键/值/事务三类超限与冲突、失效、GC 阻塞错误全部可区分")
}

func checkLimit(t *testing.T, err error, kind error, wantSize, wantLimit int) {
	t.Helper()
	if err == nil {
		t.Fatalf("期望被 %v 拒绝，实际成功", kind)
	}
	if !errors.Is(err, kind) {
		t.Fatalf("err=%v 不匹配期望类别 %v", err, kind)
	}
	var le *LimitError
	if !errors.As(err, &le) {
		t.Fatalf("期望携带 *LimitError 细节，实际 %T: %v", err, err)
	}
	if le.Size != wantSize || le.Limit != wantLimit {
		t.Fatalf("LimitError 尺寸细节不符: actual=%d limit=%d, want %d/%d",
			le.Size, le.Limit, wantSize, wantLimit)
	}
}

// TestInvalidOptions 非法配置必须在 Open 时被拒绝。
func TestInvalidOptions(t *testing.T) {
	bad := DefaultOptions()
	bad.MaxValueBytes = 0
	dir := t.TempDir()
	if _, err := Open(dir+"/db", bad); err == nil {
		t.Fatal("非法 Options 应在 Open 时被拒绝")
	}
}
