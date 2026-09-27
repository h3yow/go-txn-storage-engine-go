package storage

import "errors"

// 本文件集中定义引擎对外暴露的、彼此可区分的失败原因。
// 调用方应使用 errors.Is 判断错误类别，而不是匹配错误字符串。
var (
	// ErrWriteWriteConflict 是快照隔离下的写写冲突：
	// 本事务读快照之后，有另一个已提交事务修改了本事务将要写入的键，
	// 按 first-committer-wins 规则，后提交者被拒绝。
	ErrWriteWriteConflict = errors.New("storage: write-write conflict: key was modified by a concurrent committed transaction")

	// ErrTxnNotActive 表示事务已经失效（已提交、已回滚，或因冲突被中止），
	// 不能再在其上执行读/写操作。
	ErrTxnNotActive = errors.New("storage: transaction is no longer active (committed, rolled back, or aborted)")

	// ErrKeyTooLarge 表示单个键的字节长度超过配置上限。
	ErrKeyTooLarge = errors.New("storage: key exceeds configured maximum size")

	// ErrValueTooLarge 表示单个值的字节长度超过配置上限。
	ErrValueTooLarge = errors.New("storage: value exceeds configured maximum size")

	// ErrTxnTooLarge 表示单个事务累积的写入字节数超过配置上限。
	ErrTxnTooLarge = errors.New("storage: transaction writes exceed configured maximum size")

	// ErrGCBlocked 表示日志回收被仍在使用旧快照的读事务阻塞：
	// 回收不能删除该快照仍可能需要的日志记录。
	ErrGCBlocked = errors.New("storage: log garbage collection is blocked by an active reader holding an older snapshot")

	// ErrLogCorrupted 表示日志在非尾部位置出现无法解释的损坏。
	// 仅末尾存在半条记录时引擎会自行截断并照常启动，不会返回此错误。
	ErrLogCorrupted = errors.New("storage: log file is corrupted at a non-tail position")

	// ErrClosed 表示引擎已关闭。
	ErrClosed = errors.New("storage: engine is closed")

	// ErrNotFound 表示在事务快照中该键不存在（已删除或从未写入）。
	ErrNotFound = errors.New("storage: key not found")
)

// ConflictError 携带写写冲突的细节，errors.Is(err, ErrWriteWriteConflict) 仍然成立。
type ConflictError struct {
	Key      string // 发生冲突的键
	ReadSnap uint64 // 本事务开始时的快照版本
	Winner   uint64 // 抢先提交方的提交版本
}

func (e *ConflictError) Error() string {
	return "storage: write-write conflict on key \"" + e.Key +
		"\": snapshot=" + uitoa(e.ReadSnap) + ", committed by txn version=" + uitoa(e.Winner)
}

// Is 让 ConflictError 可被 errors.Is(..., ErrWriteWriteConflict) 命中。
func (e *ConflictError) Is(target error) bool { return target == ErrWriteWriteConflict }

// LimitError 携带违反边界约定的具体细节；具体类别由 Kind 指向对应的哨兵错误。
type LimitError struct {
	Kind   error  // ErrKeyTooLarge / ErrValueTooLarge / ErrTxnTooLarge
	Size   int    // 实际大小（字节）
	Limit  int    // 允许上限（字节）
	Detail string // 可读描述
}

func (e *LimitError) Error() string {
	return "storage: boundary violated: " + e.Detail +
		" (actual=" + itoa(e.Size) + ", limit=" + itoa(e.Limit) + ")"
}

// Is 让 LimitError 可被对应的哨兵错误命中。
func (e *LimitError) Is(target error) bool { return e.Kind == target }

// GCBlockedError 说明回收被哪个快照阻塞以及阻塞点。
type GCBlockedError struct {
	OldestSnapshot  uint64 // 当前最老的活跃读事务快照版本
	WantReclaimUpTo uint64 // 本次回收希望删除到的最高版本
}

func (e *GCBlockedError) Error() string {
	return "storage: cannot reclaim log records up to version " + uitoa(e.WantReclaimUpTo) +
		": active reader still needs snapshot " + uitoa(e.OldestSnapshot)
}

// Is 让 GCBlockedError 可被 errors.Is(..., ErrGCBlocked) 命中。
func (e *GCBlockedError) Is(target error) bool { return target == ErrGCBlocked }

func uitoa(u uint64) string {
	if u == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for u > 0 {
		i--
		b[i] = byte('0' + u%10)
		u /= 10
	}
	return string(b[i:])
}

func itoa(i int) string {
	if i < 0 {
		return "-" + uitoa(uint64(-i))
	}
	return uitoa(uint64(i))
}
