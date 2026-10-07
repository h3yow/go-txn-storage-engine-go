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

// 本测试用独立子进程驱动“提交与增量回收并发”，并在回收的不同阶段
// 通过 os.Exit（SIGKILL 语义：无 defer、无收尾 fsync）反复强杀，
// 每次重启后校验：已确认提交一条不少、保留边界可接续、可继续提交。
//
// 阶段通过环境变量 GC_CRASH_PHASE 传入；空串表示干净运行。

func TestRetentionCrashRestartLoop(t *testing.T) {
	if os.Getenv("GC_HELPER") == "1" {
		t.Skip("helper 模式下跳过外壳测试")
	}
	dir := filepath.Join(t.TempDir(), "db")
	phases := []string{
		"", // 先干净建库
		crashPhaseAfterFirstChunk,
		crashPhaseMidChunks,
		crashPhaseAfterFinalChunk,
		crashPhaseAfterDeleteSegs,
		crashPhaseMidChunks, // 再来一轮中间态，验证多次崩溃恢复
	}

	var expectByRound []int // 每轮（含干净轮）已确认提交条数
	for round, phase := range phases {
		out, code := runRetentionHelper(t, dir, phase, round, 40)
		if phase == "" {
			if code != 0 {
				t.Fatalf("干净建库阶段失败 code=%d:\n%s", code, out)
			}
			n := parseCommittedCount(out)
			expectByRound = append(expectByRound, n)
			t.Logf("[输入] 第 %d 轮（干净）: 已确认提交=%d", round, n)
		} else {
			t.Logf("[输入] 第 %d 轮在阶段 %s 强杀（退出码=%d），日志末尾:\n%s",
				round, phase, code, tailLines(out, 8))
			expectByRound = append(expectByRound, parseCommittedDelta(out))
		}

		// 重启校验：不注入、不杀；逐轮统计 seq-R-N 键，必须与确认数一致。
		db, err := Open(dir, retentionOptions(3, 2))
		if err != nil {
			t.Fatalf("[判定] 第 %d 轮崩溃后重启失败: %v", round, err)
		}
		st := db.RetentionStatus()
		stats := db.RecoveryStats()
		t.Logf("[回收水位] 第 %d 轮重启后: version=v%d floor=v%d target=v%d session=%v 检查点=%v 段=%v",
			round, db.Version(), st.HistoryFloorVersion, st.RetainTargetVersion,
			st.SessionInProgress, stats.CheckpointFound, listSegs(t, dir))

		var totalExpect, totalGot int
		for r, n := range expectByRound {
			totalExpect += n
			totalGot += countPrefixedKeys(t, db, fmt.Sprintf("seq-%d-", r), n)
		}
		t.Logf("[数据-恢复后] 累计确认提交=%d 实际可见=%d（最新版本 v%d）",
			totalExpect, totalGot, db.Version())
		if totalGot != totalExpect {
			t.Fatalf("[判定] 第 %d 轮恢复后提交条数不一致: 确认=%d 可见=%d（一条不能少）",
				round, totalExpect, totalGot)
		}
		// 边界单调不倒退：floor 不能高于当前版本；
		// 已完成的检查点版本在恢复后仍可作为历史点开。
		if st.HistoryFloorVersion > db.Version() {
			t.Fatalf("边界异常: floor=%d > version=%d", st.HistoryFloorVersion, db.Version())
		}
		if st.HistoryFloorVersion > 0 {
			v, err := db.BeginReadOnlyAt(st.HistoryFloorVersion)
			if err != nil {
				t.Fatalf("[判定] 边界版本 v%d 应仍可开视图: %v", st.HistoryFloorVersion, err)
			}
			v.Rollback()
		}
		// 恢复后回收必须能继续推进（待最终化会话被自动补做），并能继续提交。
		runGCUntil(t, db, func(s RetentionStatus) bool { return true }, 1)
		if err := db.Put(fmt.Sprintf("restart-ok-%d", round), []byte(strconv.Itoa(round))); err != nil {
			t.Fatalf("[判定] 恢复后无法继续提交: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("[结果] 多轮强杀/重启后全部已确认提交完整")
}

// countPrefixedKeys 统计 prefix0..prefix(n-1) 中存在（非墓碑）的键数。
func countPrefixedKeys(t *testing.T, db *DB, prefix string, n int) int {
	t.Helper()
	cnt := 0
	for i := 0; i < n; i++ {
		_, err := db.Get(fmt.Sprintf("%s%03d", prefix, i))
		switch {
		case err == nil:
			cnt++
		case errors.Is(err, ErrNotFound):
		default:
			t.Fatalf("读取 %s%03d 异常: %v", prefix, i, err)
		}
	}
	return cnt
}

func parseCommittedCount(out string) int {
	// 取最后一个 HELPER_TOTAL 行。
	n := 0
	for _, line := range strings.Split(out, "\n") {
		if v, ok := strings.CutPrefix(line, "HELPER_TOTAL="); ok {
			fmt.Sscanf(v, "%d", &n)
		}
	}
	return n
}

func parseCommittedDelta(out string) int {
	// helper 每写一条打印 HELPER_COMMIT seq=N；被杀前打印多少就算确认多少。
	maxN := -1
	for _, line := range strings.Split(out, "\n") {
		if v, ok := strings.CutPrefix(line, "HELPER_COMMIT="); ok {
			var n int
			if _, err := fmt.Sscanf(v, "%d", &n); err == nil && n > maxN {
				maxN = n
			}
		}
	}
	if maxN < 0 {
		return 0
	}
	return maxN + 1
}

func tailLines(out string, n int) string {
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

// runRetentionHelper 启动子进程；phase 非空时在该回收阶段 os.Exit(7)。
func runRetentionHelper(t *testing.T, dir, phase string, round, writes int) (string, int) {
	t.Helper()
	// 直接运行测试二进制：必须用 -test.* 参数形式；
	// 误传 "test" 首参会被当成测试名正则而跳过 helper。
	args := []string{"-test.run", "TestHelperRetentionCrash", "-test.count=1"}
	cmd := exec.Command(os.Args[0], args...)
	cmd.Env = append(os.Environ(),
		"GC_HELPER=1",
		"GC_DIR="+dir,
		"GC_PHASE="+phase,
		"GC_ROUND="+strconv.Itoa(round),
		"GC_WRITES="+strconv.Itoa(writes),
	)
	cmd.Dir = filepath.Dir(findModuleDir(t))
	out := new(safeBuffer)
	cmd.Stdout = out
	cmd.Stderr = out
	err := cmd.Run()
	s := out.String()
	if ee, ok := err.(*exec.ExitError); ok {
		return s, ee.ExitCode()
	}
	if err != nil {
		t.Fatalf("helper 启动失败: %v\n%s", err, s)
	}
	return s, 0
}

// TestHelperRetentionCrash 子进程入口：并发提交 + 单步回收，命中断点即退出。
func TestHelperRetentionCrash(t *testing.T) {
	if os.Getenv("GC_HELPER") != "1" {
		t.Skip("仅作为回收崩溃 helper 使用")
	}
	dir := os.Getenv("GC_DIR")
	phase := os.Getenv("GC_PHASE")
	round, _ := strconv.Atoi(os.Getenv("GC_ROUND"))
	writes, _ := strconv.Atoi(os.Getenv("GC_WRITES"))

	opts := retentionOptions(3, 2)
	db, err := Open(dir, opts)
	if err != nil {
		fmt.Println("HELPER_OPEN_ERR=" + err.Error())
		os.Exit(10)
	}

	if phase != "" {
		var once sync.Once
		db.setCrashHook(func(p string) {
			if p == phase {
				once.Do(func() {
					fmt.Printf("HELPER_CRASH_AT=%s\n", p)
					os.Stdout.Sync()
					os.Exit(7)
				})
			}
		})
	}

	// 并发：一个 goroutine 持续提交（写完即停），另一个持续一小步回收，
	// 直到写者结束。崩溃钩子会在中途 os.Exit，正常路径最终自然汇合。
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	var committedMu sync.Mutex
	committed := 0

	go func() { // writer：写满 writes 条后关闭 stop
		defer wg.Done()
		defer close(stop)
		for i := 0; i < writes; i++ {
			key := fmt.Sprintf("seq-%d-%03d", round, i)
			if err := db.Put(key, []byte(fmt.Sprintf("v%d", i))); err == nil {
				committedMu.Lock()
				if i+1 > committed {
					committed = i + 1
				}
				committedMu.Unlock()
				fmt.Printf("HELPER_COMMIT=%d\n", i)
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()
	go func() { // gc：写者运行期间一小步一小步回收；
		// 非崩溃（干净）轮次写者结束后再把回收追到当前水位，
		// 保证最后一个会话不会永远停在进行中。
		defer wg.Done()
		for i := 0; i < writes*3; i++ {
			select {
			case <-stop:
				if phase != "" {
					return
				}
				// 写者已结束且本轮不注入崩溃：追到水位为止。
				for j := 0; j < writes*4; j++ {
					r, _ := db.garbageCollectIncremental(GCOptions{MaxRounds: 1})
					s := db.RetentionStatus()
					if !s.SessionInProgress &&
						s.EffectiveTargetVersion <= s.HistoryFloorVersion &&
						r.ChunksWritten == 0 {
						return
					}
					if !s.SessionInProgress &&
						s.RetainTargetVersion <= s.HistoryFloorVersion &&
						r.ChunksWritten == 0 {
						return
					}
				}
				return
			default:
			}
			if _, err := db.GarbageCollect(GCOptions{MaxRounds: 1}); err != nil {
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()

	wg.Wait()
	committedMu.Lock()
	total := committed
	committedMu.Unlock()
	fmt.Printf("HELPER_TOTAL=%d\n", total)
	if err := db.Close(); err != nil {
		fmt.Println("HELPER_CLOSE_ERR=" + err.Error())
		os.Exit(11)
	}
}
