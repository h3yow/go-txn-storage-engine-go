package storage

import (
	"sort"
	"sync"
)

// versionedValue 是一个键的某个历史版本。
type versionedValue struct {
	version uint64 // 产生该版本的提交版本号（单调递增，从 1 开始）
	deleted bool   // true 表示这是一个删除标记
	value   []byte // 原始值；deleted 为 true 时无意义
}

// writeEntry 是一次提交对单个键的写入。
type writeEntry struct {
	key     string
	value   []byte
	deleted bool
}

// mvccStore 保存所有键的版本链。
// 每个键的切片按 version 升序追加，读操作给定一个快照版本，
// 只返回 version <= snap 的最新版本，从而得到快照隔离的一致视图。
type mvccStore struct {
	mu   sync.RWMutex
	keys map[string][]versionedValue
}

func newMVCCStore() *mvccStore {
	return &mvccStore{keys: make(map[string][]versionedValue)}
}

// getAt 返回键在快照版本 snap 下的值。
// 不存在或已在该快照中被删除时返回 ErrNotFound。
func (m *mvccStore) getAt(key string, snap uint64) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	chain := m.keys[key]
	for i := len(chain) - 1; i >= 0; i-- {
		v := chain[i]
		if v.version <= snap {
			if v.deleted {
				return nil, ErrNotFound
			}
			// 返回拷贝，避免调用方改动内部数据。
			out := make([]byte, len(v.value))
			copy(out, v.value)
			return out, nil
		}
	}
	return nil, ErrNotFound
}

// hasKeyAt 是 getAt 的布尔版，用于冲突检测中判断“某版本之后该键是否被动过”。
func (m *mvccStore) visibleAt(key string, snap uint64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	chain := m.keys[key]
	for i := len(chain) - 1; i >= 0; i-- {
		v := chain[i]
		if v.version <= snap {
			return !v.deleted
		}
	}
	return false
}

// latestVersionAtOrAfter 返回该键严格大于 after 的最早版本号；没有则返回 0。
// 用于提交期写写冲突判定：只要事务快照之后该键出现过新版本就算冲突，
// 无论那个新版本是写入还是删除——因为并发事务确实动过这份数据。
func (m *mvccStore) firstVersionAfter(key string, after uint64) uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// 版本链按版本升序排列，第一个大于 after 的版本即最小冲突版本。
	for _, v := range m.keys[key] {
		if v.version > after {
			return v.version
		}
	}
	return 0
}

// applyAt 以指定版本号把一批写入追加进版本链。
// 正常运行时版本由引擎单调分配；恢复时版本号来自日志记录。
func (m *mvccStore) applyAt(version uint64, writes []writeEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, w := range writes {
		vv := versionedValue{version: version, deleted: w.deleted}
		if !w.deleted {
			vv.value = make([]byte, len(w.value))
			copy(vv.value, w.value)
		}
		m.keys[w.key] = append(m.keys[w.key], vv)
	}
}

// reset 清空全部版本链，用于从检查点重建内存状态。
func (m *mvccStore) reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.keys = make(map[string][]versionedValue)
}

// restoreFromCheckpoint 用检查点中的全量当前态重建存储：
// 检查点里存在的键以 cpVersion 作为其版本写入，删除的键直接不出现。
func (m *mvccStore) restoreFromCheckpoint(cpVersion uint64, state map[string][]byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.keys = make(map[string][]versionedValue, len(state))
	for k, v := range state {
		val := make([]byte, len(v))
		copy(val, v)
		m.keys[k] = []versionedValue{{version: cpVersion, value: val}}
	}
}

// stateAt 导出在版本 snap 下的全量当前态，用于写检查点。
func (m *mvccStore) stateAt(snap uint64) map[string][]byte {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make(map[string][]byte, len(m.keys))
	for key, chain := range m.keys {
		for i := len(chain) - 1; i >= 0; i-- {
			v := chain[i]
			if v.version <= snap {
				if !v.deleted {
					buf := make([]byte, len(v.value))
					copy(buf, v.value)
					out[key] = buf
				}
				break
			}
		}
	}
	return out
}

// keyCount 返回当前键数量（含带删除标记的历史链），仅用于诊断与测试。
func (m *mvccStore) keyCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.keys)
}

// visibleKeysAt 返回在版本 snap 下“存在”（最新可见版本不是墓碑）的全部键，
// 按字典序排列。用于增量检查点开始时固定一份键清单：
// 清单一旦确定就不再变化（map 只增不删键，且每个键的版本链只追加），
// 后续分片即使与新提交并发进行，也只按清单逐键取 snap 时刻的值。
func (m *mvccStore) visibleKeysAt(snap uint64) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	keys := make([]string, 0, len(m.keys))
	for key, chain := range m.keys {
		for i := len(chain) - 1; i >= 0; i-- {
			v := chain[i]
			if v.version <= snap {
				if !v.deleted {
					keys = append(keys, key)
				}
				break
			}
		}
	}
	sort.Strings(keys)
	return keys
}
