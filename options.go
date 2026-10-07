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

	// RetainVersions 是历史保留范围：保留最近 N 个提交版本可回看，
	// 更早的历史允许被在线回收。0（默认）表示不启用保留策略，
	// 引擎行为与旧版完全一致（不做任何自动/增量回收）。
	RetainVersions uint64

	// ReclaimMaxVersionsPerRound 限制单轮回收水位最多推进多少个提交版本，
	// 用来摊薄回收对提交路径的阻塞时间。0 表示使用默认值。
	ReclaimMaxVersionsPerRound uint64

	// ReclaimMaxCheckpointBytes 限制单轮回收写入的检查点字节数上限，
	// 即单轮回收带来的额外磁盘占用上限。若当前全量态超过该上限，
	// 本轮回收跳过并标记为待续（不会报错、不会截断数据）。
	// 0 表示使用默认值。
	ReclaimMaxCheckpointBytes int64
}

// 回收参数的默认值（对应配置项为 0 时生效）。
const (
	defaultReclaimMaxVersionsPerRound uint64 = 128
	defaultReclaimMaxCheckpointBytes  int64  = 64 << 20 // 64 MiB
)

// DefaultOptions 返回一组偏保守、开箱即用的默认边界。
func DefaultOptions() Options {
	return Options{
		MaxKeyBytes:        1 << 10,  // 1 KiB
		MaxValueBytes:      1 << 20,  // 1 MiB
		MaxTxnWriteBytes:   16 << 20, // 16 MiB
		MaxWalSegmentBytes: 4 << 20,  // 4 MiB
		SyncOnCommit:       true,
		// RetainVersions 默认 0：不启用保留策略，行为与旧版一致。
		RetainVersions:             0,
		ReclaimMaxVersionsPerRound: defaultReclaimMaxVersionsPerRound,
		ReclaimMaxCheckpointBytes:  defaultReclaimMaxCheckpointBytes,
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
	// 回收上限为 0 时表示“取默认值”，不允许显式配成负数。
	if o.ReclaimMaxCheckpointBytes < 0 {
		return errInvalidOptions
	}
	return nil
}

// normalize 把为 0 的回收上限替换成默认值，返回可直接使用的配置。
func (o Options) normalize() Options {
	if o.ReclaimMaxVersionsPerRound == 0 {
		o.ReclaimMaxVersionsPerRound = defaultReclaimMaxVersionsPerRound
	}
	if o.ReclaimMaxCheckpointBytes == 0 {
		o.ReclaimMaxCheckpointBytes = defaultReclaimMaxCheckpointBytes
	}
	return o
}
