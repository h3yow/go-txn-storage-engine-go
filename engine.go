package engine

import (
	"path/filepath"
	"sort"
	"sync"
)

// Options 控制引擎边界与日志位置。
type Options struct {
	Dir          string // 数据目录
	MaxKeySize   int    // 超过则 Put/Delete 返回 ErrKeyTooLarge
	MaxValueSize int    // 超过则 Put 返回 ErrValueTooLarge
	MaxTxnWrites int    // 单事务最大缓冲写次数，超过返回 ErrTxnTooLarge
	GCThreshold  uint64 // 累计提交数达到该值时由调用方/自动触发 GC
}

func (o *Options) setDefaults() {
	if o.MaxKeySize <= 0 {
		o.MaxKeySize = 1024
	}
	if o.MaxValueSize <= 0 {
		o.MaxValueSize = 1 << 20
	}
	if o.MaxTxnWrites <= 0 {
		o.MaxTxnWrites = 10000
	}
}

// Engine 是本地嵌入式事务存储引擎：MVCC 快照隔离 + 追加日志持久化。
type Engine struct {
	opts Options

	mu          sync.Mutex
	mem         *memtable
	log         *wal
	nextTS      uint64         // 已分配的最大时间戳（快照与提交共用）
	active      map[uint64]int // 活跃快照 TS -> 引用计数
	commitCount uint64         // 自上次 GC 以来的提交数
	closed      bool
}

// Open 打开（必要时创建）引擎并执行崩溃恢复：
// 重放日志重建多版本数据，损坏的尾部记录已在 openWAL 中被截断。
func Open(opts Options) (*Engine, error) {
	opts.setDefaults()
	w, err := openWAL(filepath.Join(opts.Dir, "wal.log"))
	if err != nil {
		return nil, err
	}
	e := &Engine{
		opts:   opts,
		mem:    newMemtable(),
		log:    w,
		active: make(map[uint64]int),
	}
	recs, err := w.replay()
	if err != nil {
		w.close()
		return nil, err
	}
	for _, rec := range recs {
		e.mem.apply(rec.commitTS, rec.ops)
		if rec.commitTS > e.nextTS {
			e.nextTS = rec.commitTS
		}
	}
	return e, nil
}

// Begin 开启一个快照隔离事务，快照点为当前已提交的最大时间戳。
func (e *Engine) Begin(readOnly bool) (*Txn, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil, ErrEngineClosed
	}
	ts := e.nextTS
	e.active[ts]++
	return &Txn{
		eng:        e,
		snapshotTS: ts,
		writes:     make(map[string]walOp),
		readOnly:   readOnly,
	}, nil
}

// commit 在引擎锁内完成：冲突检测 -> 分配 commitTS -> 落日志 -> 应用内存。
func (e *Engine) commit(t *Txn) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return ErrEngineClosed
	}
	// 写写冲突检测：写集中的键若在事务快照之后被别人提交过，拒绝。
	for key := range t.writes {
		if e.mem.latestCommitTS(key) > t.snapshotTS {
			return &ConflictError{Key: key}
		}
	}
	e.nextTS++
	commitTS := e.nextTS
	ops := make([]walOp, 0, len(t.writes))
	for _, op := range t.writes {
		ops = append(ops, op)
	}
	// 先落日志并 fsync，再应用到内存，保证崩溃后已提交事务完整可见。
	if err := e.log.append(walRecord{commitTS: commitTS, ops: ops}); err != nil {
		return err
	}
	e.mem.apply(commitTS, ops)
	e.commitCount++
	return nil
}

// releaseSnapshot 在事务结束时释放其快照引用。
func (e *Engine) releaseSnapshot(ts uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.active[ts]--
	if e.active[ts] <= 0 {
		delete(e.active, ts)
	}
}

// minActiveSnapshot 返回仍在被读事务使用的最老快照；
// 没有活跃事务时返回当前最大时间戳（即所有旧版本都可回收）。
func (e *Engine) minActiveSnapshot() uint64 {
	if len(e.active) == 0 {
		return e.nextTS
	}
	tsList := make([]uint64, 0, len(e.active))
	for ts := range e.active {
		tsList = append(tsList, ts)
	}
	sort.Slice(tsList, func(i, j int) bool { return tsList[i] < tsList[j] })
	return tsList[0]
}

// GC 回收不再被任何活跃快照需要的旧版本，并整体重建日志文件。
// 有读事务持有老快照时，该快照仍可见的版本不会被清掉。
func (e *Engine) GC() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return ErrEngineClosed
	}
	minSnap := e.minActiveSnapshot()
	e.mem.prune(minSnap)
	if err := e.log.rewrite(e.mem.snapshotRecords()); err != nil {
		return err
	}
	e.commitCount = 0
	return nil
}

// MaybeGC 在累计提交数超过阈值时执行 GC，返回是否执行了回收。
func (e *Engine) MaybeGC() (bool, error) {
	e.mu.Lock()
	need := e.commitCount >= e.opts.GCThreshold && e.opts.GCThreshold > 0
	e.mu.Unlock()
	if !need {
		return false, nil
	}
	if err := e.GC(); err != nil {
		return false, err
	}
	return true, nil
}

// Close 关闭引擎。
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return ErrEngineClosed
	}
	e.closed = true
	return e.log.close()
}
