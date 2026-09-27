package storage

// Txn 是一个事务。它在创建瞬间固定快照版本（snapshot isolation）：
// 之后无论其他事务提交了什么，本事务的 Get 都只看得到快照之前的版本；
// 本事务自己写入的内容对自身可见（read-your-writes）。
//
// 写操作只暂存在 writes 里，直到 Commit 成功才会落日志并发布版本；
// Rollback 或创建失败不会在任何地方留下数据。
type Txn struct {
	db       *DB
	id       uint64
	readOnly bool
	snapshot uint64 // 开始时看到的最新已提交版本

	// staged 保存本事务对每个键的最终一次写入，map 保证同一键多次写只留一条。
	staged map[string]*writeEntry
	// order 保证写集顺序稳定（测试与日志可读性）。
	order      []string
	writeBytes int // 暂存写入的累计字节数，用于事务级边界判定

	done bool // 提交或回滚后置位，此后任何操作返回 ErrTxnNotActive
}

// active 检查事务是否仍可操作。
func (t *Txn) active() error {
	if t == nil || t.done {
		return ErrTxnNotActive
	}
	return nil
}

// Get 返回键在本事务一致视图下的值。
// 本事务自己写入的值对自身可见；快照视图中不存在时返回 ErrNotFound。
func (t *Txn) Get(key string) ([]byte, error) {
	if err := t.active(); err != nil {
		return nil, err
	}
	// read-your-writes：自己的暂存写入优先。
	if w, ok := t.staged[key]; ok {
		if w.deleted {
			return nil, ErrNotFound
		}
		out := make([]byte, len(w.value))
		copy(out, w.value)
		return out, nil
	}
	return t.db.store.getAt(key, t.snapshot)
}

// Put 写入一个键值。违反任何边界约定都会返回带细节的、可区分的错误：
//   - ErrKeyTooLarge   键过长
//   - ErrValueTooLarge 值过长
//   - ErrTxnTooLarge   事务累计写入过长
func (t *Txn) Put(key string, value []byte) error {
	if err := t.active(); err != nil {
		return err
	}
	if t.readOnly {
		// 只读事务不允许写：归入“事务已不能用于该目的”的失效类错误，
		// 与写写冲突明确区分。
		return ErrTxnNotActive
	}
	if len(key) > t.db.opts.MaxKeyBytes {
		return &LimitError{
			Kind: ErrKeyTooLarge, Size: len(key), Limit: t.db.opts.MaxKeyBytes,
			Detail: "key \"" + abbrev(key) + "\" too large",
		}
	}
	if len(value) > t.db.opts.MaxValueBytes {
		return &LimitError{
			Kind: ErrValueTooLarge, Size: len(value), Limit: t.db.opts.MaxValueBytes,
			Detail: "value for key \"" + abbrev(key) + "\" too large",
		}
	}

	// 预估加入这条写入后的事务总大小：同键覆盖时先退还旧值占用。
	prevSize := 0
	if old, ok := t.staged[key]; ok {
		prevSize = len(old.key) + len(old.value)
	}
	entrySize := len(key) + len(value)
	newTotal := t.writeBytes - prevSize + entrySize
	if newTotal > t.db.opts.MaxTxnWriteBytes {
		return &LimitError{
			Kind: ErrTxnTooLarge, Size: newTotal, Limit: t.db.opts.MaxTxnWriteBytes,
			Detail: "transaction accumulated too many write bytes",
		}
	}

	val := make([]byte, len(value))
	copy(val, value)

	if old, ok := t.staged[key]; ok {
		// 覆盖同键暂存项：退还旧大小，保留其在 order 中的位置。
		t.writeBytes -= len(old.key) + len(old.value)
		old.value = val
		old.deleted = false
	} else {
		t.order = append(t.order, key)
		t.staged[key] = &writeEntry{key: key, value: val}
	}
	t.writeBytes += entrySize
	return nil
}

// Delete 删除一个键（写入墓碑）。删除同样参与写写冲突检测。
func (t *Txn) Delete(key string) error {
	if err := t.active(); err != nil {
		return err
	}
	if t.readOnly {
		return ErrTxnNotActive
	}
	if len(key) > t.db.opts.MaxKeyBytes {
		return &LimitError{
			Kind: ErrKeyTooLarge, Size: len(key), Limit: t.db.opts.MaxKeyBytes,
			Detail: "delete key \"" + abbrev(key) + "\" too large",
		}
	}
	entrySize := len(key)
	prevSize := 0
	if old, ok := t.staged[key]; ok {
		prevSize = len(old.key) + len(old.value)
	}
	if t.writeBytes-prevSize+entrySize > t.db.opts.MaxTxnWriteBytes {
		return &LimitError{
			Kind:   ErrTxnTooLarge,
			Size:   t.writeBytes - prevSize + entrySize,
			Limit:  t.db.opts.MaxTxnWriteBytes,
			Detail: "transaction accumulated too many write bytes on delete",
		}
	}
	if old, ok := t.staged[key]; ok {
		t.writeBytes -= len(old.key) + len(old.value)
		old.value = nil
		old.deleted = true
	} else {
		t.order = append(t.order, key)
		t.staged[key] = &writeEntry{key: key, deleted: true}
	}
	t.writeBytes += entrySize
	return nil
}

// Commit 尝试提交。成功返回 nil；失败时事务同时被中止，错误可区分：
//   - ErrWriteWriteConflict 快照之后有并发事务改了同一批键（携带冲突键细节）
//   - ErrTxnNotActive       事务此前已经结束
func (t *Txn) Commit() error {
	if err := t.active(); err != nil {
		return err
	}
	return t.db.commitTxn(t)
}

// Rollback 放弃事务。只丢弃内存暂存，不写任何日志。
func (t *Txn) Rollback() {
	if t == nil || t.done {
		return
	}
	t.db.finishTxn(t)
}

// snapshotVersion 暴露事务看到的快照版本，供 GC 判断保护边界与测试断言。
func (t *Txn) snapshotVersion() uint64 { return t.snapshot }

// stagedWrites 按稳定顺序导出写集。
func (t *Txn) stagedWrites() []writeEntry {
	out := make([]writeEntry, 0, len(t.order))
	for _, k := range t.order {
		w := t.staged[k]
		out = append(out, writeEntry{key: w.key, value: w.value, deleted: w.deleted})
	}
	return out
}

func abbrev(s string) string {
	const max = 32
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}
