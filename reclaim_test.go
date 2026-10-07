package storage

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestReclaimRetentionBoundary 覆盖保留范围边界：
// 回收推进到“最近 N 版”下沿后，边界之外过期、边界之上可看；
// 重启后边界与数据都保持。
func TestReclaimRetentionBoundary(t *testing.T) {
	opts := tinySegmentOptions()
	opts.RetainVersions = 3
	dir := filepath.Join(t.TempDir(), "db")

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	commitSeq(t, db, "k", "v%d", 1, 12) // v1..v12
	t.Logf("[输入] 已提交 v1..v%d，保留窗口=最近 %d 版", db.Version(), opts.RetainVersions)

	// 手动驱动回收到底（后台循环也在跑，二者殊途同归）。
	for {
		res, err := db.ReclaimLog()
		if err != nil {
			t.Fatal(err)
		}
		if !res.Skipped {
			t.Logf("[回收] 水位=v%d 边界 %d→%d 删段=%d 检查点=%dB",
				res.Horizon, res.FloorBefore, res.FloorAfter, res.SegmentsRemoved, res.CheckpointBytes)
		}
		if res.Skipped {
			break
		}
	}
	info := db.RetentionInfo()
	t.Logf("[判定] 回收后：边界=v%d 最新=v%d pending=%v", info.HistoryFloor, info.CurrentVersion, info.PendingReclaim)
	if info.HistoryFloor != 10 { // 12-3+1
		t.Fatalf("[判定] 边界应推进到 v10，实际 v%d", info.HistoryFloor)
	}
	if info.PendingReclaim {
		t.Fatal("[判定] 窗口外不应再有待回收历史")
	}

	// 边界之外过期，边界之上可看。
	if _, err := db.BeginReadOnlyAt(9); !errors.Is(err, ErrSnapshotExpired) {
		t.Fatalf("[判定] v9 已出保留范围，应过期，实际 %v", err)
	}
	view, err := db.BeginReadOnlyAt(10)
	if err != nil {
		t.Fatalf("[判定] v10 在保留范围内，应能开启: %v", err)
	}
	if got, _ := view.Get("k"); string(got) != "v10" {
		t.Fatalf("[判定] v10 视图应读到 v10，实际 %s", got)
	}
	view.Rollback()
	t.Log("[判定] 边界 v9 过期、v10 可看，符合保留窗口语义")

	// 记录回收后、重启前的完整状态用于对比。
	before := map[string]string{}
	for _, key := range []string{"k"} {
		v, _ := db.Get(key)
		before[key] = string(v)
	}
	floorBefore := db.RetentionInfo().HistoryFloor
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// 重启：边界与数据都要接上。
	db2, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	info2 := db2.RetentionInfo()
	t.Logf("[结果] 重启后：边界=v%d（重启前 v%d）最新=v%d 检查发现点=%v",
		info2.HistoryFloor, floorBefore, info2.CurrentVersion, db2.RecoveryStats().CheckpointFound)
	if info2.HistoryFloor != floorBefore {
		t.Fatalf("[判定] 重启后历史边界应接续为 v%d，实际 v%d", floorBefore, info2.HistoryFloor)
	}
	for key, want := range before {
		got, err := db2.Get(key)
		if err != nil || string(got) != want {
			t.Fatalf("[判定] 重启后 %s=%q err=%v，期望 %q", key, got, err, want)
		}
	}
	t.Logf("[结果] 重启前后数据一致: %v", before)
	if _, err := db2.BeginReadOnlyAt(9); !errors.Is(err, ErrSnapshotExpired) {
		t.Fatalf("[判定] 重启后 v9 仍应过期，实际 %v", err)
	}
}

// TestReclaimRoundBounds 覆盖单轮回收工作量上限：
// 每轮推进的版本数不超过 ReclaimMaxVersionsPerRound。
func TestReclaimRoundBounds(t *testing.T) {
	opts := tinySegmentOptions()
	opts.RetainVersions = 0 // 手动驱动，目标水位=最新版本
	opts.ReclaimMaxVersionsPerRound = 2
	dir := filepath.Join(t.TempDir(), "db")

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	commitSeq(t, db, "k", "v%d", 1, 10) // v1..v10
	t.Logf("[输入] 已提交 v1..v%d，单轮推进上限=%d 版", db.Version(), opts.ReclaimMaxVersionsPerRound)

	rounds := 0
	for {
		res, err := db.ReclaimLog()
		if err != nil {
			t.Fatal(err)
		}
		if res.Skipped {
			break
		}
		rounds++
		step := res.FloorAfter - res.FloorBefore
		t.Logf("[回收] 第 %d 轮：边界 %d→%d（步长 %d）删段=%d", rounds, res.FloorBefore, res.FloorAfter, step, res.SegmentsRemoved)
		if step > opts.ReclaimMaxVersionsPerRound {
			t.Fatalf("[判定] 单轮推进 %d 版，超过上限 %d", step, opts.ReclaimMaxVersionsPerRound)
		}
		if res.CheckpointBytes > 0 && int64(res.CheckpointBytes) > opts.ReclaimMaxCheckpointBytes {
			t.Fatalf("[判定] 单轮检查点 %dB 超过上限 %dB", res.CheckpointBytes, opts.ReclaimMaxCheckpointBytes)
		}
	}
	if got := db.RetentionInfo().HistoryFloor; got != 10 {
		t.Fatalf("[判定] 边界最终应推进到 v10，实际 v%d", got)
	}
	if rounds < 2 {
		t.Fatalf("[判定] 上限为 2 时推进 10 个版本应需多轮，实际 %d 轮", rounds)
	}
	t.Logf("[判定] 共 %d 轮，每轮步长均 <= %d，边界推进到 v10", rounds, opts.ReclaimMaxVersionsPerRound)
}

// TestReclaimAutoResumesAfterReader 覆盖后台自动回收：
// 老读者活着时水位被钳制在某个点，读者结束后回收自动继续推进，
// 不需要任何手动触发。
func TestReclaimAutoResumesAfterReader(t *testing.T) {
	opts := tinySegmentOptions()
	opts.RetainVersions = 2
	dir := filepath.Join(t.TempDir(), "db")

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	commitSeq(t, db, "k", "v%d", 1, 2) // v1,v2
	// 在窗口被超出之前开老读者，钉住 v2。
	reader, err := db.BeginReadOnlyAt(2)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("[输入] 开启老读者，钉住快照 v%d", reader.snapshotVersion())

	commitSeq(t, db, "k", "v%d", 3, 6) // v3..v8
	t.Logf("[输入] 继续提交到 v%d，保留窗口=最近 2 版", db.Version())

	// 后台回收应自动推进，但被老读者钳制在 v2。
	waitFor(t, 5*time.Second, func() bool { return db.RetentionInfo().HistoryFloor == 2 })
	t.Logf("[判定] 老读者存活期间，后台回收推进到 v2 后被钳制（最老读者=v%d）",
		db.RetentionInfo().OldestReader)

	// 再提交几个版本，边界不能越过 v2。
	commitSeq(t, db, "k", "v%d", 9, 2) // v9,v10
	time.Sleep(200 * time.Millisecond)
	if got := db.RetentionInfo().HistoryFloor; got != 2 {
		t.Fatalf("[判定] 老读者存活期间边界不得越过 v2，实际 v%d", got)
	}
	t.Log("[判定] 继续提交后边界仍钳制在 v2，没有整体卡住也没有越过读者")

	// 读者结束：后台回收应自动继续推进到窗口下沿 v9（10-2+1）。
	reader.Rollback()
	waitFor(t, 5*time.Second, func() bool { return db.RetentionInfo().HistoryFloor == 9 })
	t.Logf("[判定] 读者结束后后台回收自动推进到 v%d（窗口下沿 v9）", db.RetentionInfo().HistoryFloor)
}

// TestReclaimConcurrentWithCommits 覆盖回收与提交并发（race 检测下）：
// 并发提交、并发开历史视图、并发回收，已提交数据一条不丢。
func TestReclaimConcurrentWithCommits(t *testing.T) {
	opts := tinySegmentOptions()
	opts.RetainVersions = 4
	dir := filepath.Join(t.TempDir(), "db")

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const writers = 4
	const perWriter = 25
	done := make(chan struct{})

	// 写者：各自写互不相同的键（避免写写冲突干扰断言）。
	var writersWG sync.WaitGroup
	for g := 0; g < writers; g++ {
		writersWG.Add(1)
		go func(g int) {
			defer writersWG.Done()
			for i := 0; i < perWriter; i++ {
				key := fmt.Sprintf("w%d-%02d", g, i)
				if err := db.Put(key, []byte(fmt.Sprintf("val-%d-%d", g, i))); err != nil {
					t.Errorf("put %s: %v", key, err)
					return
				}
			}
		}(g)
	}

	// 回收者与读者：直到写者全部完成后才退出。
	var bgWG sync.WaitGroup

	// 回收者：持续手动轮次（与后台循环、提交并发）。
	bgWG.Add(1)
	go func() {
		defer bgWG.Done()
		for {
			select {
			case <-done:
				return
			default:
			}
			if _, err := db.ReclaimLog(); err != nil && !errors.Is(err, ErrClosed) {
				t.Errorf("reclaim: %v", err)
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	// 读者：反复在当前保留范围内开历史视图读最新键。
	bgWG.Add(1)
	go func() {
		defer bgWG.Done()
		for {
			select {
			case <-done:
				return
			default:
			}
			info := db.RetentionInfo()
			at := info.HistoryFloor
			if at < info.CurrentVersion {
				at = info.CurrentVersion // 看最新提交点
			}
			view, err := db.BeginReadOnlyAt(at)
			if err != nil {
				continue // 边界并发推进时可能过期，重试即可
			}
			_, _ = view.Get("w0-00")
			view.Rollback()
		}
	}()

	writersWG.Wait()
	close(done)
	bgWG.Wait()

	// 最终把回收推到底，然后校验全部已提交键都在。
	for {
		res, _ := db.ReclaimLog()
		if res.Skipped {
			break
		}
	}
	info := db.RetentionInfo()
	t.Logf("[结果] 并发结束：最新版本=v%d 边界=v%d（窗口下沿 v%d）",
		info.CurrentVersion, info.HistoryFloor, info.CurrentVersion-info.RetainVersions+1)
	missing := 0
	for g := 0; g < writers; g++ {
		for i := 0; i < perWriter; i++ {
			key := fmt.Sprintf("w%d-%02d", g, i)
			want := fmt.Sprintf("val-%d-%d", g, i)
			got, err := db.Get(key)
			if err != nil || string(got) != want {
				t.Errorf("已提交数据丢失: %s=%q err=%v，期望 %q", key, got, err, want)
				missing++
			}
		}
	}
	if missing == 0 {
		t.Logf("[判定] %d 个并发已提交键全部完整", writers*perWriter)
	}
}

// waitFor 轮询条件直到满足或超时。
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("等待条件超时")
}

// ---------------- 强杀重启测试（子进程） ----------------

// TestReclaimCrashRecoveryLoop 覆盖：回收与提交并发进行时反复强杀，
// 已返回成功的提交一条不少，保留边界与回收进度重启后接续，
// 回收重来幂等不把结果搞乱。
func TestReclaimCrashRecoveryLoop(t *testing.T) {
	if os.Getenv("RECLAIM_HELPER") == "1" {
		t.Skip("helper 模式下跳过外壳测试")
	}
	dir := filepath.Join(t.TempDir(), "db")

	var prevMaxFloor uint64
	for cycle := 1; cycle <= 3; cycle++ {
		committed, floor := runReclaimHelper(t, dir, 40, 3*time.Second)
		t.Logf("[输入] 第 %d 轮：强杀前子进程已确认提交到 iter=%d，回收边界推进到 v%d",
			cycle, committed, floor)

		// 强杀后重启校验。
		db, err := Open(dir, reclaimCrashOptions())
		if err != nil {
			t.Fatalf("[判定] 第 %d 轮强杀后重启失败: %v", cycle, err)
		}
		st := db.RecoveryStats()
		info := db.RetentionInfo()
		t.Logf("[判定] 第 %d 轮重启：扫描段=%d 发现检查点=%v(v%d) 最新版本=v%d 历史边界=v%d",
			cycle, st.SegmentsScanned, st.CheckpointFound, st.CheckpointVer,
			info.CurrentVersion, info.HistoryFloor)

		// 1) 已确认提交一条不少：counter >= 强杀前最后确认的 iter。
		got, err := db.Get("counter")
		if committed >= 0 {
			if err != nil {
				t.Fatalf("[判定] counter 必须存在（已有确认提交），err=%v", err)
			}
			n, _ := strconv.Atoi(string(got))
			if n < committed {
				t.Fatalf("[判定] 已确认提交丢失：counter=%d，强杀前已确认 iter=%d", n, committed)
			}
			t.Logf("[判定] counter=%d ≥ 强杀前已确认 iter=%d，已提交数据完整", n, committed)
		}
		// 2) 回收进度接续：边界单调不回退。
		if info.HistoryFloor < prevMaxFloor || info.HistoryFloor < floor {
			t.Fatalf("[判定] 历史边界回退：重启后 v%d，此前已推进到 v%d", info.HistoryFloor, floor)
		}
		t.Logf("[判定] 历史边界 v%d 接续此前进度 v%d，未回退", info.HistoryFloor, floor)

		// 3) 回收重来幂等：重启后再跑回收不出错、数据不变。
		wantCounter := string(got)
		for {
			res, err := db.ReclaimLog()
			if err != nil {
				t.Fatalf("[判定] 重启后回收重来失败: %v", err)
			}
			if res.Skipped {
				break
			}
			t.Logf("[回收] 重启后续跑：边界 %d→%d 删段=%d", res.FloorBefore, res.FloorAfter, res.SegmentsRemoved)
		}
		if got2, _ := db.Get("counter"); string(got2) != wantCounter {
			t.Fatalf("[判定] 回收重来后数据变化：%s → %s", wantCounter, got2)
		}
		t.Logf("[判定] 重启后回收重来幂等，counter 恒为 %s", wantCounter)

		prevMaxFloor = db.RetentionInfo().HistoryFloor
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("[结果] 3 轮强杀-重启校验通过：提交不丢、边界不回退、回收幂等")
}

func reclaimCrashOptions() Options {
	o := tinySegmentOptions()
	o.RetainVersions = 4
	return o
}

// runReclaimHelper 拉起子进程并发执行“提交+回收”，写途中 SIGKILL，
// 解析输出返回强杀前最后确认的提交序号与回收边界。
func runReclaimHelper(t *testing.T, dir string, iters int, wait time.Duration) (lastCommitted int, lastFloor uint64) {
	t.Helper()
	args := []string{"test", "-run", "TestHelperReclaimCrashProcess", "-count=1", "."}
	cmd := exec.Command(os.Args[0], args...)
	cmd.Env = append(os.Environ(),
		"RECLAIM_HELPER=1",
		"RECLAIM_DIR="+dir,
		"RECLAIM_ITERS="+strconv.Itoa(iters),
	)
	cmd.Dir = findModuleDir(t)
	out := new(safeBuffer)
	cmd.Stdout = out
	cmd.Stderr = out
	if err := cmd.Start(); err != nil {
		t.Fatalf("启动 reclaim helper 失败: %v", err)
	}

	// 等到子进程至少推进了一些提交与回收，然后强杀。
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		if strings.Contains(out.String(), "HELPER_RECLAIM") {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond) // 让它再跑一小段，杀在“回收与提交并发”的任意位置
	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("SIGKILL: %v", err)
	}
	_ = cmd.Wait()

	lastCommitted = -1
	for _, line := range strings.Split(out.String(), "\n") {
		if n, ok := strings.CutPrefix(line, "HELPER_COMMITTED "); ok {
			if v, err := strconv.Atoi(strings.TrimSpace(n)); err == nil {
				lastCommitted = v
			}
		}
		if f, ok := strings.CutPrefix(line, "HELPER_RECLAIM floor="); ok {
			fields := strings.Fields(f)
			if v, err := strconv.ParseUint(fields[0], 10, 64); err == nil {
				lastFloor = v
			}
		}
	}
	t.Logf("[输入] helper 输出尾部:\n%s", tailLines(out.String(), 6))
	return lastCommitted, lastFloor
}

func tailLines(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

// TestHelperReclaimCrashProcess 是被强杀的子进程入口：并发提交 + 回收。
func TestHelperReclaimCrashProcess(t *testing.T) {
	if os.Getenv("RECLAIM_HELPER") != "1" {
		t.Skip("仅作为崩溃 helper 使用")
	}
	dir := os.Getenv("RECLAIM_DIR")
	iters, _ := strconv.Atoi(os.Getenv("RECLAIM_ITERS"))

	db, err := Open(dir, reclaimCrashOptions())
	if err != nil {
		t.Fatalf("helper open: %v", err)
	}
	// 回收与提交并发：后台一个 goroutine 不停跑回收轮次。
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			res, err := db.ReclaimLog()
			if err == nil && !res.Skipped {
				fmt.Printf("HELPER_RECLAIM floor=%d removed=%d\n", res.FloorAfter, res.SegmentsRemoved)
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()

	for i := 0; i < iters; i++ {
		if err := db.Put("counter", []byte(strconv.Itoa(i))); err != nil {
			t.Fatalf("helper put: %v", err)
		}
		fmt.Printf("HELPER_COMMITTED %d\n", i)
		time.Sleep(5 * time.Millisecond) // 放慢，让外层能杀在任意位置
	}
	close(stop)
	fmt.Println("HELPER_DONE")
}
