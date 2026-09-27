package storage

import (
	"sync"
	"testing"
)

// TestSnapshotIsolationUnderConcurrentWrites 验证并发读写互不干扰：
// 读事务在其生命周期内反复读，必须始终看到它开始时的一致视图，
// 绝不能读到其他事务的中途状态（多版本链保证无半值可见）。
func TestSnapshotIsolationUnderConcurrentWrites(t *testing.T) {
	db, _ := newTestDB(t, DefaultOptions())
	defer db.Close()

	if err := db.Put("k", []byte("snap0")); err != nil {
		t.Fatal(err)
	}

	reader, err := db.BeginReadOnly()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Rollback()

	var wg sync.WaitGroup
	stop := make(chan struct{})
	firstCommit := make(chan struct{})

	// 写者：持续提交新版本；先同步保证至少一次提交落地，避免调度竞态。
	wg.Add(1)
	go func() {
		defer wg.Done()
		committed := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			wx, _ := db.Begin()
			_ = wx.Put("k", []byte("changing"))
			if err := wx.Commit(); err != nil {
				t.Errorf("writer commit: %v", err)
				return
			}
			committed++
			if committed == 1 {
				close(firstCommit)
			}
		}
	}()
	<-firstCommit

	// 读者：在老快照上反复读取，必须永远稳定。
	for i := 0; i < 500; i++ {
		v, err := reader.Get("k")
		if err != nil || string(v) != "snap0" {
			t.Fatalf("快照视图被并发写污染: v=%q err=%v（应恒为 snap0）", v, err)
		}
	}
	close(stop)
	wg.Wait()

	// 新读事务看到的是最新值，老视图与新视图分明。
	fresh, _ := db.BeginReadOnly()
	defer fresh.Rollback()
	v, _ := fresh.Get("k")
	if string(v) != "changing" {
		t.Fatalf("新快照应看到最新值 changing，实际 %q", v)
	}
	t.Logf("[判定] 老快照恒为 snap0、新快照为 changing；并发读写互不干扰，版本=%d", db.Version())
}
