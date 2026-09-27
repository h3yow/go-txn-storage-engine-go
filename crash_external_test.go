package storage

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// 本测试通过 `go test` 重新拉起一个独立进程执行写入，再用 SIGKILL 强杀，
// 验证真实崩溃路径下：已在 Commit 返回前 fsync 的事务重启后完整可见。
//
// 子进程入口是本文件的 TestHelperCrashProcess（仅在 CRASH_HELPER=1 时执行写动作）。

func TestRealProcessKillDurability(t *testing.T) {
	if os.Getenv("CRASH_HELPER") == "1" {
		t.Skip("helper 模式下跳过外壳测试")
	}
	dir := filepath.Join(t.TempDir(), "db")

	// 第一阶段：干净写入 3 条并正常退出，它们必须 100% 持久。
	runHelper(t, dir, []string{"durable-1=A", "durable-2=B", "durable-3=C"}, "", 0)

	// 第二阶段：子进程再写 5 条，写入途中 SIGKILL。
	// 已返回成功的那些必须可见；没返回的可能不存在，但绝不能出现半截值。
	start := time.Now()
	runHelper(t, dir, []string{"maybe-1=1", "maybe-2=2", "maybe-3=3", "maybe-4=4", "maybe-5=5"},
		"KILL_AFTER=2", 3*time.Second)
	t.Logf("[输入] 子进程在写入途中被 SIGKILL，耗时 %v", time.Since(start))

	db, err := Open(dir, DefaultOptions())
	if err != nil {
		t.Fatalf("[判定] 强杀后重启失败: %v", err)
	}
	defer db.Close()

	st := db.RecoveryStats()
	t.Logf("[判定] 强杀后恢复: 版本=v%d 扫描段=%d 截断段=%s 截断字节=%d",
		db.Version(), st.SegmentsScanned, st.TruncatedSegment, st.TruncatedBytes)

	// 第一批：必须全部完整可见（恢复前后对比）。
	before := map[string]string{"durable-1": "A", "durable-2": "B", "durable-3": "C"}
	for k, want := range before {
		v, err := db.Get(k)
		if err != nil || string(v) != want {
			t.Fatalf("已提交数据丢失/损坏: %s=%q err=%v，期望 %s", k, v, err, want)
		}
	}
	t.Logf("[结果] 强杀前已确认提交的数据恢复后完整: %v", before)

	// 第二批：逐个检查。值只能是完整的期望值或不存在，绝不允许出现截断/错值。
	for i := 1; i <= 5; i++ {
		k := fmt.Sprintf("maybe-%d", i)
		want := fmt.Sprintf("%d", i)
		v, err := db.Get(k)
		switch {
		case errors.Is(err, ErrNotFound):
			t.Logf("[结果] %s 在强杀前未完成提交，恢复后不存在（符合预期）", k)
		case err == nil && string(v) == want:
			t.Logf("[结果] %s=%s 已在强杀前 fsync，恢复后完整可见", k, v)
		default:
			t.Fatalf("出现不允许的半截状态: %s=%q err=%v（期望完整值 %s 或不存在）", k, v, err, want)
		}
	}
}

// runHelper 启动一个外部测试二进制执行写入。killAfter>0 表示写完前 N 条后强杀。
func runHelper(t *testing.T, dir string, pairs []string, killAfter string, wait time.Duration) {
	t.Helper()
	args := []string{"test", "-run", "TestHelperCrashProcess", "-count=1", "."}
	cmd := exec.Command(os.Args[0], args...)
	cmd.Env = append(os.Environ(),
		"CRASH_HELPER=1",
		"CRASH_DIR="+dir,
		"CRASH_PAIRS="+strings.Join(pairs, ","),
		"CRASH_KILL_AFTER="+killAfter,
	)
	cmd.Dir = filepath.Dir(findModuleDir(t))
	out := new(safeBuffer)
	cmd.Stdout = out
	cmd.Stderr = out

	if err := cmd.Start(); err != nil {
		t.Fatalf("启动 helper 失败: %v", err)
	}

	if killAfter != "" {
		// 等子进程打印 READY，再给它一点时间写入后 SIGKILL。
		deadline := time.Now().Add(wait)
		for time.Now().Before(deadline) && !strings.Contains(out.String(), "HELPER_WRITING") {
			time.Sleep(5 * time.Millisecond)
		}
		time.Sleep(10 * time.Millisecond)
		if err := cmd.Process.Kill(); err != nil {
			t.Fatalf("SIGKILL: %v", err)
		}
		_ = cmd.Wait()
		t.Logf("[输入] helper 输出（被强杀）:\n%s", out.String())
		return
	}

	if err := cmd.Wait(); err != nil {
		t.Fatalf("helper 正常阶段失败: %v\n%s", err, out.String())
	}
}

func findModuleDir(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return wd
}

// TestHelperCrashProcess 是被强杀的子进程入口。
func TestHelperCrashProcess(t *testing.T) {
	if os.Getenv("CRASH_HELPER") != "1" {
		t.Skip("仅作为崩溃 helper 使用")
	}
	dir := os.Getenv("CRASH_DIR")
	pairs := strings.Split(os.Getenv("CRASH_PAIRS"), ",")
	killMode := os.Getenv("CRASH_KILL_AFTER") != ""

	db, err := Open(dir, DefaultOptions())
	if err != nil {
		t.Fatalf("helper open: %v", err)
	}

	fmt.Println("HELPER_WRITING")
	for _, p := range pairs {
		kv := strings.SplitN(p, "=", 2)
		if err := db.Put(kv[0], []byte(kv[1])); err != nil {
			t.Fatalf("helper put %s: %v", kv[0], err)
		}
		fmt.Printf("HELPER_COMMITTED %s=%s (v%d)\n", kv[0], kv[1], db.Version())
		if killMode {
			// 放慢写入，让外层能在任意两次（或某次）写盘途中杀掉本进程。
			time.Sleep(15 * time.Millisecond)
		}
	}
	// 正常阶段：关闭退出；被强杀的阶段走不到这里。
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	fmt.Println("HELPER_DONE_CLEAN")
}

// safeBuffer 是给子进程 stdout/stderr 使用的线程安全缓冲：
// 父进程轮询内容的同时，管道写入 goroutine 仍可能向它追加。
type safeBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *safeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
