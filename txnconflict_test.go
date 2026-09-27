package storage

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

// TestConcurrentWriteWriteConflict 覆盖需求场景：
// 两个事务基于同一个快照改同一个键，先提交者成功，
// 后提交者必须被拒绝，且错误能被明确识别为“写写冲突”，
// 而不是笼统的失败或“事务失效”。
func TestConcurrentWriteWriteConflict(t *testing.T) {
	db, _ := newTestDB(t, DefaultOptions())
	defer db.Close()

	// 初始数据：k=base，两个事务都从版本 1 的快照开始。
	if err := db.Put("k", []byte("base")); err != nil {
		t.Fatal(err)
	}
	baseVersion := db.Version()
	t.Logf("[输入] 初始数据 k=base, 已提交版本=%d", baseVersion)

	const n = 8
	var wg sync.WaitGroup
	var success, conflict, otherFail int
	var mu sync.Mutex
	var detail *ConflictError

	// 先在任何提交发生前创建全部事务，使它们固定在同一个快照上，
	// 这才能精确模拟“同一视图下并发改同一份数据”的场景。
	txs := make([]*Txn, n)
	for i := 0; i < n; i++ {
		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("begin %d: %v", i, err)
		}
		val := fmt.Sprintf("writer-%d", i)
		if err := tx.Put("k", []byte(val)); err != nil {
			t.Fatalf("writer %d Put: %v", i, err)
		}
		if tx.snapshotVersion() != baseVersion {
			t.Fatalf("writer %d 快照=%d，期望全部固定在 v%d", i, tx.snapshotVersion(), baseVersion)
		}
		txs[i] = tx
		t.Logf("[输入] writer-%d 基于快照 v%d 暂存 k=%s", i, tx.snapshotVersion(), val)
	}

	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		tx := txs[i]
		go func() {
			defer wg.Done()
			err := tx.Commit()
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				success++
				t.Logf("[判定] writer-%d 提交成功（first committer）", i)
			case errors.Is(err, ErrWriteWriteConflict):
				conflict++
				var ce *ConflictError
				if errors.As(err, &ce) {
					detail = ce
					t.Logf("[判定] writer-%d 被拒绝=写写冲突: 键=%s 我的快照=v%d 赢家版本=v%d",
						i, ce.Key, ce.ReadSnap, ce.Winner)
				}
			default:
				otherFail++
				t.Errorf("[判定] writer-%d 非预期错误: %v", i, err)
			}
		}()
	}
	wg.Wait()

	t.Logf("[结果] 成功=%d 写写冲突拒绝=%d 其他失败=%d", success, conflict, otherFail)

	if success != 1 {
		t.Fatalf("期望恰有 1 个提交成功，实际成功 %d", success)
	}
	if conflict != n-1 {
		t.Fatalf("期望 %d 个因写写冲突被拒，实际 %d", n-1, conflict)
	}
	if otherFail != 0 {
		t.Fatalf("不允许出现不可区分的其他失败，实际 %d", otherFail)
	}
	if detail == nil {
		t.Fatal("期望至少捕获一个带细节的 ConflictError")
	}
	if detail.Key != "k" || detail.ReadSnap != baseVersion || detail.Winner <= baseVersion {
		t.Fatalf("ConflictError 细节不合理: %+v", detail)
	}

	// 被拒绝的事务之后必须处于“已失效”状态，再用它要得到 ErrTxnNotActive。
	// 另开一对事务显式验证错误类别的可区分性。
	t1, _ := db.Begin()
	t2, _ := db.Begin()
	if err := t1.Put("k", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := t1.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := t2.Put("k", []byte("y")); err != nil {
		t.Fatal(err)
	}
	err := t2.Commit()
	if !errors.Is(err, ErrWriteWriteConflict) {
		t.Fatalf("后提交事务 err=%v, 期望 ErrWriteWriteConflict", err)
	}
	// t2 因冲突已被引擎中止。
	if _, err := t2.Get("k"); !errors.Is(err, ErrTxnNotActive) {
		t.Fatalf("冲突后再使用事务 err=%v, 期望 ErrTxnNotActive", err)
	}
	t.Logf("[判定] 冲突事务再次使用 -> ErrTxnNotActive，两类原因可区分")
}

// TestNoConflictOnDisjointKeys 确保边界正确：不同键的并发写互不冲突。
func TestNoConflictOnDisjointKeys(t *testing.T) {
	db, _ := newTestDB(t, DefaultOptions())
	defer db.Close()

	t1, _ := db.Begin()
	t2, _ := db.Begin()
	if err := t1.Put("a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := t2.Put("b", []byte("2")); err != nil {
		t.Fatal(err)
	}
	if err := t1.Commit(); err != nil {
		t.Fatalf("t1 commit: %v", err)
	}
	if err := t2.Commit(); err != nil {
		t.Fatalf("不相交键不应冲突，t2 commit: %v", err)
	}
	if v, _ := db.Get("a"); string(v) != "1" {
		t.Fatalf("a=%q want 1", v)
	}
	if v, _ := db.Get("b"); string(v) != "2" {
		t.Fatalf("b=%q want 2", v)
	}
	t.Log("[判定] 不相交键的两个事务均提交成功，无写写冲突")
}
