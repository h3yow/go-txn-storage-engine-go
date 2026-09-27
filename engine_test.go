package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func testOptions(t *testing.T) Options {
	t.Helper()
	return Options{
		Dir:          t.TempDir(),
		MaxKeySize:   16,
		MaxValueSize: 64,
		MaxTxnWrites: 4,
	}
}

// 场景 1：两个并发事务改同一个键，后提交者必须被识别为写写冲突，
// 且要与"事务已失效"这类错误区分开。
func TestConcurrentCommitConflict(t *testing.T) {
	eng, err := Open(testOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	// 初始数据
	seed, _ := eng.Begin(false)
	seed.Put("account", []byte("100"))
	if err := seed.Commit(); err != nil {
		t.Fatal(err)
	}

	// 两个事务从同一快照出发，都改 account
	txA, _ := eng.Begin(false)
	txB, _ := eng.Begin(false)
	txA.Put("account", []byte("80"))
	txB.Put("account", []byte("60"))

	var wg sync.WaitGroup
	errs := make([]error, 2)
	// 并发提交，顺序不定，但必须恰好一个成功、一个冲突
	wg.Add(2)
	go func() { defer wg.Done(); errs[0] = txA.Commit() }()
	go func() { defer wg.Done(); errs[1] = txB.Commit() }()
	wg.Wait()

	t.Logf("输入: txA 写 account=80, txB 写 account=60, 二者快照相同")
	t.Logf("结果: txA.Commit err=%v, txB.Commit err=%v", errs[0], errs[1])

	var succ, conflict int
	for _, err := range errs {
		if err == nil {
			succ++
			continue
		}
		var ce *ConflictError
		if errors.As(err, &ce) {
			conflict++
			t.Logf("判定: %v 是写写冲突(ConflictError), 冲突键=%s", err, ce.Key)
		} else {
			t.Fatalf("判定失败: 期望 ConflictError, 得到 %T %v", err, err)
		}
	}
	if succ != 1 || conflict != 1 {
		t.Fatalf("判定: 应恰好 1 成功 1 冲突, 实际 succ=%d conflict=%d", succ, conflict)
	}

	// 区分错误类型：对已关闭事务操作应得到 ErrTxnClosed 而非冲突
	err = txA.Put("x", []byte("y"))
	t.Logf("对已提交事务再写入: err=%v", err)
	if !errors.Is(err, ErrTxnClosed) {
		t.Fatalf("判定失败: 期望 ErrTxnClosed, 得到 %v", err)
	}
	if errors.Is(err, ErrNotFound) {
		t.Fatal("判定失败: ErrTxnClosed 不应与 ErrNotFound 混淆")
	}

	// 胜出的值必须可见
	chk, _ := eng.Begin(true)
	v, err := chk.Get("account")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("提交后读到的 account=%s (应为 80 或 60 之一)", v)
	if string(v) != "80" && string(v) != "60" {
		t.Fatalf("意外值 %q", v)
	}
	chk.Rollback()
}

// 场景 2：读事务持有老快照期间发生日志回收，
// 老快照视图不能被回收破坏；回收后重启数据仍一致。
func TestGCDuringActiveReadTxn(t *testing.T) {
	eng, err := Open(testOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	put := func(k, v string) {
		tx, _ := eng.Begin(false)
		if err := tx.Put(k, []byte(v)); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	put("k1", "v1-old")
	put("k2", "v2")

	// 读事务在此刻拿快照
	reader, _ := eng.Begin(true)
	before, err := reader.Get("k1")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("输入: 读事务快照中 k1=%s", before)

	// 之后 k1 被多次覆盖，旧版本成为回收候选
	put("k1", "v1-mid")
	put("k1", "v1-new")
	put("k2", "v2-new")

	// 读事务仍活跃时执行 GC
	if err := eng.GC(); err != nil {
		t.Fatal(err)
	}
	t.Log("已在读事务活跃期间执行 GC")

	// 判定依据：老快照必须仍读到 v1-old，而不是被回收掉
	after, err := reader.Get("k1")
	if err != nil {
		t.Fatalf("判定失败: GC 后老快照读 k1 出错: %v", err)
	}
	t.Logf("判定: GC 前后老快照读到 k1: %s -> %s (应一致)", before, after)
	if string(before) != string(after) {
		t.Fatalf("判定失败: GC 破坏了活跃快照, %q != %q", before, after)
	}
	reader.Rollback()

	// 新事务应读到最新值
	fresh, _ := eng.Begin(true)
	v, _ := fresh.Get("k1")
	t.Logf("判定: 新事务读到 k1=%s (应为 v1-new)", v)
	if string(v) != "v1-new" {
		t.Fatalf("判定失败: 新事务读到 %q", v)
	}
	fresh.Rollback()

	// 回收后重启，数据应与回收前一致
	dir := eng.opts.Dir
	eng.Close()
	eng2, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer eng2.Close()
	chk, _ := eng2.Begin(true)
	v1, _ := chk.Get("k1")
	v2, _ := chk.Get("k2")
	t.Logf("判定: GC 后重启读到 k1=%s k2=%s (应 v1-new / v2-new)", v1, v2)
	if string(v1) != "v1-new" || string(v2) != "v2-new" {
		t.Fatalf("判定失败: 重启后 k1=%q k2=%q", v1, v2)
	}
	chk.Rollback()
}

// 场景 3：日志尾部有一条写了一半的记录，重启时引擎应识别并跳过，
// 已提交事务完整可见，半截写入不留痕迹。
func TestTornTailRecovery(t *testing.T) {
	opts := testOptions(t)
	eng, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}

	put := func(k, v string) {
		tx, _ := eng.Begin(false)
		tx.Put(k, []byte(v))
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	put("a", "1")
	put("b", "2")

	// 恢复前的数据视图
	pre, _ := eng.Begin(true)
	preA, _ := pre.Get("a")
	preB, _ := pre.Get("b")
	t.Logf("恢复前: a=%s b=%s", preA, preB)
	pre.Rollback()
	eng.Close()

	// 模拟崩溃：在日志末尾拼上半条记录（完整头部 + 不完整的 payload）
	walPath := filepath.Join(opts.Dir, "wal.log")
	f, err := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	torn := []byte{0x40, 0x00, 0x00, 0x00, 0xDE, 0xAD, 0xBE, 0xEF, 0x03, 0x00} // len=64 但只写了 2 字节 payload
	n, _ := f.Write(torn)
	f.Close()
	t.Logf("输入: 在日志尾部追加 %d 字节半条记录(声称 payload=64B, 实际只有 2B)", n)

	// 重启：应正常打开并截断损坏尾部
	eng2, err := Open(opts)
	if err != nil {
		t.Fatalf("判定失败: 尾部损坏应可恢复, 但 Open 报错: %v", err)
	}
	defer eng2.Close()

	post, _ := eng2.Begin(true)
	postA, errA := post.Get("a")
	postB, errB := post.Get("b")
	_, errC := post.Get("nonexistent")
	t.Logf("恢复后: a=%s(err=%v) b=%s(err=%v)", postA, errA, postB, errB)

	// 判定依据：恢复前后已提交数据必须一致
	if string(postA) != string(preA) || string(postB) != string(preB) {
		t.Fatalf("判定失败: 恢复前后不一致 (%s,%s) vs (%s,%s)", postA, postB, preA, preB)
	}
	if !errors.Is(errC, ErrNotFound) {
		t.Fatalf("判定失败: 不存在的键应返回 ErrNotFound, 得到 %v", errC)
	}
	post.Rollback()

	// 恢复后还能继续写入并再次重启可见
	tx, _ := eng2.Begin(false)
	tx.Put("c", []byte("3"))
	if err := tx.Commit(); err != nil {
		t.Fatalf("判定失败: 截断后应能继续追加日志: %v", err)
	}
	eng2.Close()
	eng3, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng3.Close()
	chk, _ := eng3.Begin(true)
	c, _ := chk.Get("c")
	t.Logf("判定: 二次重启读到 c=%s (应为 3)", c)
	if string(c) != "3" {
		t.Fatalf("判定失败: c=%q", c)
	}
	chk.Rollback()
}

// 场景 4：边界保护，各类超限要返回各自明确的错误。
func TestBoundaryErrors(t *testing.T) {
	eng, err := Open(testOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	tx, _ := eng.Begin(false)

	bigKey := string(make([]byte, 17)) // MaxKeySize=16
	err = tx.Put(bigKey, []byte("v"))
	t.Logf("输入: key 长度 17 > 上限 16 -> err=%v", err)
	if !errors.Is(err, ErrKeyTooLarge) {
		t.Fatalf("判定失败: 期望 ErrKeyTooLarge, 得到 %v", err)
	}

	bigVal := make([]byte, 65) // MaxValueSize=64
	err = tx.Put("k", bigVal)
	t.Logf("输入: value 长度 65 > 上限 64 -> err=%v", err)
	if !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("判定失败: 期望 ErrValueTooLarge, 得到 %v", err)
	}

	// MaxTxnWrites=4，写第 5 个不同键应被拒
	for i := 0; i < 4; i++ {
		if err := tx.Put(fmt.Sprintf("k%d", i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	err = tx.Put("k4", []byte("v"))
	t.Logf("输入: 事务内第 5 个键 > 上限 4 -> err=%v", err)
	if !errors.Is(err, ErrTxnTooLarge) {
		t.Fatalf("判定失败: 期望 ErrTxnTooLarge, 得到 %v", err)
	}

	// 三种错误必须互不相同
	if errors.Is(ErrKeyTooLarge, ErrValueTooLarge) || errors.Is(ErrValueTooLarge, ErrTxnTooLarge) {
		t.Fatal("判定失败: 边界错误之间不应互相匹配")
	}
	tx.Rollback()
}

// 场景 5：快照隔离 —— 读事务期间并发写入不可见。
func TestSnapshotIsolation(t *testing.T) {
	eng, err := Open(testOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	tx, _ := eng.Begin(false)
	tx.Put("k", []byte("v1"))
	tx.Commit()

	reader, _ := eng.Begin(true)
	writer, _ := eng.Begin(false)
	writer.Put("k", []byte("v2"))
	writer.Commit()

	v, _ := reader.Get("k")
	t.Logf("输入: 读事务快照后 k 被改为 v2; 判定: 读事务看到 k=%s (应仍为 v1)", v)
	if string(v) != "v1" {
		t.Fatalf("判定失败: 快照被并发写污染, 读到 %q", v)
	}
	reader.Rollback()

	fresh, _ := eng.Begin(true)
	v2, _ := fresh.Get("k")
	t.Logf("判定: 新事务读到 k=%s (应为 v2)", v2)
	if string(v2) != "v2" {
		t.Fatalf("判定失败: 读到 %q", v2)
	}
	fresh.Rollback()
}
