package engine

import "errors"

// 各类明确的错误原因，调用方可用 errors.Is 区分。
var (
	ErrKeyTooLarge   = errors.New("engine: key exceeds max key size")
	ErrValueTooLarge = errors.New("engine: value exceeds max value size")
	ErrTxnTooLarge   = errors.New("engine: transaction exceeds max buffered writes")
	ErrTxnClosed     = errors.New("engine: transaction already committed or rolled back")
	ErrNotFound      = errors.New("engine: key not found")
	ErrEngineClosed  = errors.New("engine: engine is closed")
)

// ConflictError 表示提交时的写写冲突，携带冲突的键。
type ConflictError struct {
	Key string
}

func (e *ConflictError) Error() string {
	return "engine: write-write conflict on key " + strconvQuote(e.Key)
}

func strconvQuote(s string) string { return `"` + s + `"` }
