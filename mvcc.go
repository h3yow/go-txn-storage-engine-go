package engine

// version 是某个键的一次已提交版本。
type version struct {
	commitTS uint64
	value    []byte
	deleted  bool
}

// memtable 保存所有键的多版本数据，每个键的版本按 commitTS 升序排列。
type memtable struct {
	data map[string][]version
}

func newMemtable() *memtable { return &memtable{data: make(map[string][]version)} }

// get 返回在 snapshotTS 时刻可见的版本（commitTS <= snapshotTS 的最新版本）。
func (m *memtable) get(key string, snapshotTS uint64) (version, bool) {
	vers := m.data[key]
	// 版本按 commitTS 升序，从后往前找第一个 <= snapshotTS 的。
	for i := len(vers) - 1; i >= 0; i-- {
		if vers[i].commitTS <= snapshotTS {
			if vers[i].deleted {
				return version{}, false
			}
			return vers[i], true
		}
	}
	return version{}, false
}

// latestCommitTS 返回该键最新已提交版本的 commitTS，无则 0。
// 用于写写冲突检测：若 latestCommitTS > 事务快照，说明快照之后有人改过。
func (m *memtable) latestCommitTS(key string) uint64 {
	vers := m.data[key]
	if len(vers) == 0 {
		return 0
	}
	return vers[len(vers)-1].commitTS
}

// apply 把一次提交的写入追加为新版本（commitTS 单调递增）。
func (m *memtable) apply(commitTS uint64, ops []walOp) {
	for _, op := range ops {
		m.data[op.key] = append(m.data[op.key], version{
			commitTS: commitTS,
			value:    op.value,
			deleted:  op.delete,
		})
	}
}

// prune 删除所有活跃快照都不再需要的旧版本：
// 对每个键，保留 commitTS > minSnapshot 的全部版本，
// 以及 <= minSnapshot 的最新一个版本（最老快照仍可能读它）。
func (m *memtable) prune(minSnapshot uint64) {
	for key, vers := range m.data {
		cut := 0
		for i, v := range vers {
			if v.commitTS <= minSnapshot {
				cut = i // 记录 <= minSnapshot 的最后一个下标
			}
		}
		if cut > 0 {
			m.data[key] = append([]version(nil), vers[cut:]...)
		}
		if len(m.data[key]) == 1 && m.data[key][0].deleted {
			// 只剩一个 tombstone 且没有更早的快照需要它：整个键移除。
			delete(m.data, key)
		}
	}
}

// snapshotRecords 导出回收后仍需写入日志的记录：
// 每个键保留 prune 之后仍存活的版本（含 tombstone，防止旧数据复活）。
func (m *memtable) snapshotRecords() []walRecord {
	// 按 commitTS 聚合，保证重放后版本顺序不变。
	var recs []walRecord
	for key, vers := range m.data {
		for _, v := range vers {
			recs = append(recs, walRecord{
				commitTS: v.commitTS,
				ops:      []walOp{{delete: v.deleted, key: key, value: v.value}},
			})
		}
	}
	// 稳定排序：按 commitTS 升序。
	for i := 1; i < len(recs); i++ {
		for j := i; j > 0 && recs[j-1].commitTS > recs[j].commitTS; j-- {
			recs[j-1], recs[j] = recs[j], recs[j-1]
		}
	}
	return recs
}
