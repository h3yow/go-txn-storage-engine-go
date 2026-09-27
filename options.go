package storage

import "errors"

// Options 是引擎的可配置边界与行为。
// 所有“超过约定边界必须被明确拒绝”的规则都来自这里，
// 零值 Options 不可直接使用，请用 DefaultOptions 后按需覆盖。
type Options struct {
	// MaxKeyBytes 单个键允许的最大字节数。
	MaxKeyBytes int

	// MaxValueBytes 单个值允许的最大字节数。
	MaxValueBytes int

	// MaxTxnWriteBytes 单个事务内所有键/值写入的累计字节上限。
	MaxTxnWriteBytes int

	// MaxWalSegmentBytes 单个日志段文件达到该大小后滚动到下一段。
	MaxWalSegmentBytes int64

	// SyncOnCommit 为 true 时每次提交都 fsync 日志后才返回成功。
	// 生产环境必须保持 true；测试崩溃恢复语义时也必须为 true。
	SyncOnCommit bool
}

// DefaultOptions 返回一组偏保守、开箱即用的默认边界。
func DefaultOptions() Options {
	return Options{
		MaxKeyBytes:        1 << 10,  // 1 KiB
		MaxValueBytes:      1 << 20,  // 1 MiB
		MaxTxnWriteBytes:   16 << 20, // 16 MiB
		MaxWalSegmentBytes: 4 << 20,  // 4 MiB
		SyncOnCommit:       true,
	}
}

// errInvalidOptions 表示配置本身非法。
var errInvalidOptions = errors.New("storage: invalid options: all size limits must be positive")

// validate 对 Options 做基本合法性校验。
func (o Options) validate() error {
	if o.MaxKeyBytes <= 0 || o.MaxValueBytes <= 0 ||
		o.MaxTxnWriteBytes <= 0 || o.MaxWalSegmentBytes <= 0 {
		return errInvalidOptions
	}
	return nil
}
