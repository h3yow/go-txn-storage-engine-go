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

	// RetainVersions 配置历史保留范围：保留最近多少个提交版本可回看。
	// 0（默认）表示不启用历史视图与增量回收，GarbageCollect 维持
	// “无老读者才整体回收”的旧行为，保证已有用法与数据目录完全兼容。
	// >0 时：可在 [最近完成检查点版本, 当前版本] 内开启历史视图，
	// GC 按增量、有界的方式把检查点推进到 version-RetainVersions。
	RetainVersions int

	// MaxGCPerRoundKeys 限制单次 GC 调用一轮最多落盘的键数。
	// <=0 时使用内置默认值（见 DefaultOptions）。
	MaxGCPerRoundKeys int

	// MaxGCPerRoundBytes 限制单次 GC 调用一轮最多落盘的键值字节数，
	// 同时也是单轮额外磁盘占用的上限量级。<=0 时使用内置默认值。
	MaxGCPerRoundBytes int

	// EnableAutoGC 为 true 且 RetainVersions>0 时，引擎启动一个后台
	// goroutine 按有界小步自动推进回收；每次提交只唤醒、不做重活。
	EnableAutoGC bool
}

// DefaultOptions 返回一组偏保守、开箱即用的默认边界。
func DefaultOptions() Options {
	return Options{
		MaxKeyBytes:        1 << 10,  // 1 KiB
		MaxValueBytes:      1 << 20,  // 1 MiB
		MaxTxnWriteBytes:   16 << 20, // 16 MiB
		MaxWalSegmentBytes: 4 << 20,  // 4 MiB
		SyncOnCommit:       true,

		RetainVersions:     0,       // 默认不启用，保持旧行为
		MaxGCPerRoundKeys:  4096,    // 单轮落盘键数上限
		MaxGCPerRoundBytes: 1 << 20, // 单轮落盘字节上限（1 MiB）
		EnableAutoGC:       false,
	}
}

// errInvalidOptions 表示配置本身非法。
var errInvalidOptions = errors.New("storage: invalid options: all size limits must be positive")

// gcRoundDefaults 在调用方/配置未指定单轮上限时使用。
const (
	defaultGCRoundKeys  = 4096
	defaultGCRoundBytes = 1 << 20 // 1 MiB
)

// validate 对 Options 做基本合法性校验。
func (o Options) validate() error {
	if o.MaxKeyBytes <= 0 || o.MaxValueBytes <= 0 ||
		o.MaxTxnWriteBytes <= 0 || o.MaxWalSegmentBytes <= 0 {
		return errInvalidOptions
	}
	if o.RetainVersions < 0 || o.MaxGCPerRoundKeys < 0 || o.MaxGCPerRoundBytes < 0 {
		return errInvalidOptions
	}
	return nil
}

// gcRoundLimits 返回生效的单轮回收工作量上限（零值取内置默认）。
func (o Options) gcRoundLimits() (maxKeys int, maxBytes int) {
	maxKeys = o.MaxGCPerRoundKeys
	if maxKeys <= 0 {
		maxKeys = defaultGCRoundKeys
	}
	maxBytes = o.MaxGCPerRoundBytes
	if maxBytes <= 0 {
		maxBytes = defaultGCRoundBytes
	}
	return maxKeys, maxBytes
}
