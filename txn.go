package engine

import "errors"

// Txn 是一个快照隔离事务：从 Begin 那一刻起看到一致的数据视图。
type Txn struct {
	eng        *Engine
	snapshotTS uint64
	writes     map[string]walOp // 缓冲的写入，提交时一并生效
	done       bool
	readOnly   bool
}

// Get 先查自己的写缓冲，再读快照时刻的可见版本。
func (t *Txn) Get(key string) ([]byte, error) {
	if t.done {
		return nil, ErrTxnClosed
	}
	if op, ok := t.writes[key]; ok {
		if op.delete {
			return nil, ErrNotFound
		}
		return append([]byte(nil), op.value...), nil
	}
	t.eng.mu.Lock()
	v, ok := t.eng.mem.get(key, t.snapshotTS)
	t.eng.mu.Unlock()
	if !ok {
		return nil, ErrNotFound
	}
	return append([]byte(nil), v.value...), nil
}

// Put 缓冲一次写入，提交时生效。
func (t *Txn) Put(key string, value []byte) error {
	if err := t.checkWritable(); err != nil {
		return err
	}
	if len(key) > t.eng.opts.MaxKeySize {
		return ErrKeyTooLarge
	}
	if len(value) > t.eng.opts.MaxValueSize {
		return ErrValueTooLarge
	}
	if _, exists := t.writes[key]; !exists && len(t.writes) >= t.eng.opts.MaxTxnWrites {
		return ErrTxnTooLarge
	}
	t.writes[key] = walOp{key: key, value: append([]byte(nil), value...)}
	return nil
}

// Delete 缓冲一次删除，提交时生效。
func (t *Txn) Delete(key string) error {
	if err := t.checkWritable(); err != nil {
		return err
	}
	if len(key) > t.eng.opts.MaxKeySize {
		return ErrKeyTooLarge
	}
	if _, exists := t.writes[key]; !exists && len(t.writes) >= t.eng.opts.MaxTxnWrites {
		return ErrTxnTooLarge
	}
	t.writes[key] = walOp{delete: true, key: key}
	return nil
}

func (t *Txn) checkWritable() error {
	if t.done {
		return ErrTxnClosed
	}
	if t.readOnly {
		return errors.New("engine: read-only transaction cannot write")
	}
	return nil
}

// Commit 做写写冲突检测，然后先落日志（fsync）再应用到内存。
// 冲突时返回 *ConflictError，可用 errors.As 提取冲突键。
func (t *Txn) Commit() error {
	if t.done {
		return ErrTxnClosed
	}
	defer func() { t.done = true; t.eng.releaseSnapshot(t.snapshotTS) }()
	if len(t.writes) == 0 {
		return nil // 纯读事务无需落日志
	}
	return t.eng.commit(t)
}

// Rollback 放弃缓冲的写入并释放快照。
func (t *Txn) Rollback() {
	if t.done {
		return
	}
	t.done = true
	t.writes = nil
	t.eng.releaseSnapshot(t.snapshotTS)
}
