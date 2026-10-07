package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// 磁盘日志布局
// ----------------
// 目录由若干定长滚动的段文件组成，按序号排序即日志时间顺序：
//
//	wal-000001.log
//	wal-000002.log
//	...
//
// 每个段由一连串“帧”组成，单帧布局（大端序）：
//
//	+---------+------------+-----------+---------+
//	| magic 4B| payloadLen | payload   | crc32 4B|
//	| "WALF"  | uint32 (4B)| payloadLen| IEEE    |
//	+---------+------------+-----------+---------+
//
// crc32 覆盖 magic||payloadLen||payload。
// 进程在一次 write(2) 中途被强杀，只会在日志末尾留下：
//   - 不足 9 字节的帧头残片；或
//   - 帧头完整但 payload/crc 不全；或
//   - 帧完整但 CRC 不匹配（写穿到一半的极端情况）。
//
// 恢复时识别最后一个段末尾的这种残帧并截断，引擎照常启动；
// 若残帧出现在非末尾段（其后还有有效日志），则判定为真正损坏并拒绝启动。
const (
	walMagic   = "WALF"
	walHdrSize = 8 // magic(4) + payloadLen(4)
	walCrcSize = 4
	walFrmOver = walHdrSize + walCrcSize

	// 单帧 payload 的防御性上限，避免畸形长度字段导致巨量分配。
	walMaxPayload = 64 << 20 // 64 MiB

	recTypeCommit          = 1
	recTypeCheckpoint      = 2
	recTypeChunkCheckpoint = 3

	// cpFlagFirst/cpFlagFinal 标记分片检查点记录在会话中的位置。
	cpFlagFirst byte = 1 << 0
	cpFlagFinal byte = 1 << 1
)

// commitRec 是一条“事务提交”日志记录。
type commitRec struct {
	version uint64
	writes  []writeEntry
}

// checkpointRec 是一条“检查点”日志记录，携带某版本下的全量键值态。
type checkpointRec struct {
	version uint64
	state   map[string][]byte
}

// chunkCheckpointRec 是增量回收使用的“分片检查点”记录。
// 一次完整的增量检查点由 session 相同、seq 从 0 连续递增的多条记录组成：
//   - first=true 的记录携带目标版本并强制落在一个全新段的开头；
//   - 中间记录只携带若干键值；
//   - final=true 的记录补齐最后一批键值。
//
// 只有 first 与 final 齐全、seq 连续的 session 才在恢复时生效；
// 写到一半被强杀的残 session 直接忽略（其所在旧段稍后仍可被下次回收删除）。
type chunkCheckpointRec struct {
	session uint64 // 本次检查点的唯一标识（取目标版本，单调递增）
	version uint64 // 检查点目标版本（仅 first 记录可信）
	seq     uint32 // 分片序号，从 0 开始
	first   bool   // 是否为该 session 的首个分片
	final   bool   // 是否为该 session 的末个分片
	entries []writeEntry
}

// recoveryStats 描述本次启动恢复都识别/丢弃了什么，供诊断与测试断言。
type recoveryStats struct {
	SegmentsScanned  int    // 扫描过的段文件数
	CommitRecords    int    // 解析出的提交记录数
	CheckpointFound  bool   // 是否发现检查点
	CheckpointVer    uint64 // 检查点版本
	TruncatedSegment string // 被截断尾部残帧的段文件名（空表示未截断）
	TruncatedBytes   int64  // 截掉的字节数
}

// wal 是追加日志。所有写方法互斥；读路径只在启动恢复时使用。
type wal struct {
	dir  string
	opts Options

	mu     sync.Mutex
	seq    int      // 当前活动段序号
	f      *os.File // 当前活动段文件
	offset int64    // 当前段已写入字节
}

func walName(seq int) string {
	return fmt.Sprintf("wal-%06d.log", seq)
}

// openWAL 打开（必要时创建）日志目录，执行恢复扫描与残尾截断，
// 最后把活动段定位到序号最大的段上以便继续追加。
func openWAL(dir string, opts Options) (*wal, recoveryStats, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, recoveryStats{}, fmt.Errorf("storage: mkdir wal dir: %w", err)
	}

	names, err := listWalSegments(dir)
	if err != nil {
		return nil, recoveryStats{}, err
	}

	stats := recoveryStats{SegmentsScanned: len(names)}

	// 逐段扫描；只有最后一个段允许出现“末尾残帧”。
	for idx, name := range names {
		path := filepath.Join(dir, name)
		goodBytes, tornBytes, perr := inspectSegment(path)
		if perr != nil {
			return nil, stats, perr
		}
		if tornBytes > 0 {
			if idx != len(names)-1 {
				return nil, stats, fmt.Errorf("%w: valid data exists after a bad frame in %s",
					ErrLogCorrupted, name)
			}
			// 末尾段：物理截断残尾。
			if err := truncateFile(path, goodBytes); err != nil {
				return nil, stats, err
			}
			stats.TruncatedSegment = name
			stats.TruncatedBytes = tornBytes
		}
	}

	// 打开/创建活动段。
	seq := 1
	if len(names) > 0 {
		seq = parseWalSeq(names[len(names)-1])
	}
	path := filepath.Join(dir, walName(seq))
	// O_APPEND 至关重要：恢复时（尤其在残尾被物理截断之后）重新打开段，
	// 新帧必须接在现存有效字节之后，绝不能从偏移 0 覆盖已提交的帧。
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, stats, fmt.Errorf("storage: open active segment: %w", err)
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, stats, err
	}
	// offset 是当前段的逻辑尾；残尾截断后 fi.Size() 正好是有效帧总长度。

	// 新建段时目录项也需要持久化，崩溃后文件才“确实存在”。
	if len(names) == 0 {
		_ = syncDir(dir)
	}

	return &wal{dir: dir, opts: opts, seq: seq, f: f, offset: fi.Size()}, stats, nil
}

// inspectSegment 顺序解析一个段的所有帧，
// 返回全部帧占用的字节数 goodBytes，以及末尾残帧的字节数 tornBytes。
// 帧完整但 CRC 错误同样视为残帧（崩溃写穿的极端情形）。
func inspectSegment(path string) (goodBytes int64, tornBytes int64, err error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, 0, fmt.Errorf("storage: read segment %s: %w", path, err)
	}
	pos := 0
	n := len(data)
	for pos < n {
		remain := n - pos
		if remain < walFrmOver {
			return int64(pos), int64(remain), nil // 帧头/CRC 不完整
		}
		if string(data[pos:pos+4]) != walMagic {
			return int64(pos), int64(remain), nil // magic 不匹配：从此处起为残尾
		}
		payloadLen := int(binary.BigEndian.Uint32(data[pos+4 : pos+8]))
		if payloadLen > walMaxPayload {
			return int64(pos), int64(remain), nil // 长度字段不可信，按残尾处理
		}
		frameLen := walFrmOver + payloadLen
		if remain < frameLen {
			return int64(pos), int64(remain), nil // payload 或 crc 不完整
		}
		wantCRC := binary.BigEndian.Uint32(data[pos+frameLen-4 : pos+frameLen])
		gotCRC := crc32.ChecksumIEEE(data[pos : pos+frameLen-4])
		if wantCRC != gotCRC {
			return int64(pos), int64(remain), nil // CRC 不匹配：帧写了一半
		}
		pos += frameLen
	}
	return int64(pos), 0, nil
}

// readAllRecords 按时间顺序读取所有段中的完整日志记录。
// 必须在残尾截断之后调用。
//
// 返回值中的 chunkCP 是从“分片检查点”记录装配出的完整检查点；
// cp 是旧格式的整段检查点。二者可能同时出现（升级/混用目录），
// 调用方取版本更高者作为恢复基线。
func (w *wal) readAllRecords() (commits []commitRec, cp *checkpointRec, chunkCP *checkpointRec, stats recoveryStats, err error) {
	names, err := listWalSegments(w.dir)
	if err != nil {
		return nil, nil, nil, stats, err
	}
	stats.SegmentsScanned = len(names)

	// 增量检查点会话的在线装配状态。
	type chunkSession struct {
		version uint64
		expect  uint32 // 下一个期望的分片序号
		state   map[string][]byte
	}
	var sessions = make(map[uint64]*chunkSession)
	var completed []*checkpointRec // 按完成顺序收集

	flushCompleted := func(session uint64) {
		s := sessions[session]
		if s == nil {
			return
		}
		completed = append(completed, &checkpointRec{version: s.version, state: s.state})
		delete(sessions, session)
	}

	for _, name := range names {
		data, rerr := os.ReadFile(filepath.Join(w.dir, name))
		if rerr != nil {
			return nil, nil, nil, stats, rerr
		}
		pos := int64(0)
		for pos < int64(len(data)) {
			payload, frameLen, ok := readFrame(data[pos:])
			if !ok {
				// openWAL 已截断残尾，这里再遇到只能是非尾部损坏。
				return nil, nil, nil, stats, fmt.Errorf("%w: unexpected bad frame in %s at offset %d",
					ErrLogCorrupted, name, pos)
			}
			pos += int64(frameLen)
			switch payload[0] {
			case recTypeCommit:
				rec, cerr := decodeCommit(payload)
				if cerr != nil {
					return nil, nil, nil, stats, fmt.Errorf("%w: %v in %s at offset %d",
						ErrLogCorrupted, cerr, name, pos)
				}
				commits = append(commits, rec)
				stats.CommitRecords++
			case recTypeCheckpoint:
				c, cerr := decodeCheckpoint(payload)
				if cerr != nil {
					return nil, nil, nil, stats, fmt.Errorf("%w: %v in %s at offset %d",
						ErrLogCorrupted, cerr, name, pos)
				}
				cp = c // 后出现的整段检查点覆盖先出现的（正常至多一个）
			case recTypeChunkCheckpoint:
				rec, cerr := decodeChunkCheckpoint(payload)
				if cerr != nil {
					return nil, nil, nil, stats, fmt.Errorf("%w: %v in %s at offset %d",
						ErrLogCorrupted, cerr, name, pos)
				}
				// first 必须开启一个全新会话；非 first 必须落在已开会话上；
				// seq 必须从 1 连续。但要容忍一种“良性不完整”：
				// 已完成检查点会话的首片段所在旧段可能在之前的回收中被删，
				// 日志里只剩它中后段的残留分片——这些会话已被更新的检查点取代，
				// 直接忽略，绝不当作日志损坏。
				if rec.first {
					if rec.seq != 0 {
						return nil, nil, nil, stats, fmt.Errorf(
							"%w: chunk checkpoint session %d first record has seq %d",
							ErrLogCorrupted, rec.session, rec.seq)
					}
					sessions[rec.session] = &chunkSession{
						version: rec.version, expect: 1,
						state: make(map[string][]byte),
					}
				} else {
					s := sessions[rec.session]
					if s == nil {
						// 首片段已随旧段删除：忽略被取代会话的残留分片。
						continue
					}
					if rec.seq != s.expect {
						// 会话内部出现缺口（正常滚动/删除不会产生）：
						// 丢弃这个无法完整装配的会话。
						delete(sessions, rec.session)
						continue
					}
					s.expect++
				}
				s := sessions[rec.session]
				if s == nil {
					continue
				}
				for _, e := range rec.entries {
					if !e.deleted {
						s.state[e.key] = e.value
					}
				}
				if rec.final {
					flushCompleted(rec.session)
				}
			default:
				return nil, nil, nil, stats, fmt.Errorf("%w: unknown record type %d in %s",
					ErrLogCorrupted, payload[0], name)
			}
		}
	}

	// 未完成（崩溃时写到一半）的会话一律忽略：它们前面的旧段不可能已被删除
	// （删除只发生在 final 之后），因此从更早基线恢复不会丢数据。
	for _, c := range completed {
		if chunkCP == nil || c.version > chunkCP.version {
			chunkCP = c
		}
	}
	if cp != nil {
		stats.CheckpointFound = true
		stats.CheckpointVer = cp.version
	}
	if chunkCP != nil && (cp == nil || chunkCP.version > cp.version) {
		stats.CheckpointFound = true
		stats.CheckpointVer = chunkCP.version
	}
	return commits, cp, chunkCP, stats, nil
}

// readFrame 从缓冲区开头解析一帧，返回 payload 与整帧长度。
func readFrame(buf []byte) (payload []byte, frameLen int, ok bool) {
	if len(buf) < walFrmOver || string(buf[:4]) != walMagic {
		return nil, 0, false
	}
	payloadLen := int(binary.BigEndian.Uint32(buf[4:8]))
	frameLen = walHdrSize + payloadLen + walCrcSize
	if payloadLen > walMaxPayload || len(buf) < frameLen {
		return nil, 0, false
	}
	want := binary.BigEndian.Uint32(buf[frameLen-4 : frameLen])
	if crc32.ChecksumIEEE(buf[:frameLen-4]) != want {
		return nil, 0, false
	}
	return buf[walHdrSize : walHdrSize+payloadLen], frameLen, true
}

// appendPayload 编码一帧并追加到日志，必要时滚动新段；提交成功语义要求 sync。
func (w *wal) appendPayload(payload []byte, sync bool) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	frame := encodeFrame(payload)
	needed := int64(len(frame))
	if w.opts.MaxWalSegmentBytes > 0 &&
		w.offset > 0 && w.offset+needed > w.opts.MaxWalSegmentBytes {
		if err := w.rotateLocked(); err != nil {
			return err
		}
	}
	if _, err := w.f.Write(frame); err != nil {
		return fmt.Errorf("storage: write wal frame: %w", err)
	}
	w.offset += needed
	if sync {
		if err := w.f.Sync(); err != nil {
			return fmt.Errorf("storage: fsync wal: %w", err)
		}
	}
	return nil
}

// finishedChunkSession 描述一个已出现 final 分片的检查点会话。
type finishedChunkSession struct {
	version    uint64 // 会话标识（=检查点目标版本）
	startSeq   int    // 首片所在段序号
	finalSeq   int    // 末片所在段序号（删段安全边界）
	finalFound bool
}

// finishedChunkSessions 返回日志中所有“已出现 final 分片”的分片检查点
// 会话，按版本（=时间）升序。用于崩溃恢复时判断最新检查点是否已完成
// “删段/推边界”动作，以及重建删段边界。
func (w *wal) finishedChunkSessions() ([]finishedChunkSession, error) {
	names, nerr := listWalSegments(w.dir)
	if nerr != nil {
		return nil, nerr
	}
	finished := make(map[uint64]bool)
	starts := make(map[uint64]int)
	finals := make(map[uint64]int)
	for _, name := range names {
		data, rerr := os.ReadFile(filepath.Join(w.dir, name))
		if rerr != nil {
			return nil, rerr
		}
		segSeq := parseWalSeq(name)
		pos := 0
		for pos < len(data) {
			payload, frameLen, ok := readFrame(data[pos:])
			if !ok {
				break
			}
			pos += frameLen
			if payload[0] != recTypeChunkCheckpoint {
				continue
			}
			rec, derr := decodeChunkCheckpoint(payload)
			if derr != nil {
				return nil, fmt.Errorf("%w: %v", ErrLogCorrupted, derr)
			}
			if rec.first {
				starts[rec.session] = segSeq
			}
			if rec.final {
				finished[rec.session] = true
				// 末片被强制写在一个全新段开头；同一会话不会有多个 final。
				finals[rec.session] = segSeq
			}
		}
	}
	var out []finishedChunkSession
	for s := range finished {
		out = append(out, finishedChunkSession{
			version:    s,
			startSeq:   starts[s],
			finalSeq:   finals[s],
			finalFound: finals[s] > 0,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].version < out[j].version })
	return out, nil
}

// rotateIntoNewSegment 强制滚动到新段，用于把检查点放进一个全新段的开头。
func (w *wal) rotateIntoNewSegment(payload []byte) (newSegSeq int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.rotateLocked(); err != nil {
		return 0, err
	}
	frame := encodeFrame(payload)
	if _, err := w.f.Write(frame); err != nil {
		return 0, fmt.Errorf("storage: write checkpoint frame: %w", err)
	}
	w.offset += int64(len(frame))
	if err := w.f.Sync(); err != nil {
		return 0, fmt.Errorf("storage: fsync checkpoint: %w", err)
	}
	if err := syncDir(w.dir); err != nil {
		return 0, err
	}
	return w.seq, nil
}

// startChunkSession 强制滚动到一个全新段，并把分片检查点会话的首片
// （first=true）写在该段开头。这样“删除本会话之前的旧段”永远不会
// 删掉承载会话首片的段，恢复装配总能找到起点。
func (w *wal) startChunkSession(payload []byte) (newSegSeq int, err error) {
	return w.rotateIntoNewSegment(payload)
}

// finalizeChunkInNewSegment 强制滚动到一个全新段，并把会话的末片
// （final=true）原子地写在该段开头，返回新段序号。
//
// 这是回收删除安全性的关键：末片之后的任何提交都在“检查点目标版本之后”，
// 它们要么落在末片所在段（同段追加），要么落在更后的段；因此最终化只删除
// “末片段之前”的段即可，绝不会误删检查点之后已确认的提交。
func (w *wal) finalizeChunkInNewSegment(payload []byte) (newSegSeq int, err error) {
	return w.rotateIntoNewSegment(payload)
}

// activeSeq 返回当前活动段序号（GC 据此知道哪些段比它老）。
func (w *wal) activeSeq() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.seq
}

// segmentContents 描述一个段里与回收判定相关的内容摘要。
type segmentContents struct {
	seq              int    // 段序号
	maxCommitVersion uint64 // 段内提交记录的最大版本号（无提交为 0）
	chunkSessions    map[uint64]struct{}
}

// inspectSegmentContents 扫描一个段，返回其内容摘要。
func inspectSegmentContents(path string) (segmentContents, error) {
	c := segmentContents{seq: parseWalSeq(filepath.Base(path)), chunkSessions: map[uint64]struct{}{}}
	data, err := os.ReadFile(path)
	if err != nil {
		return c, err
	}
	pos := 0
	for pos < len(data) {
		payload, frameLen, ok := readFrame(data[pos:])
		if !ok {
			break
		}
		pos += frameLen
		switch payload[0] {
		case recTypeCommit:
			if r, derr := decodeCommit(payload); derr == nil && r.version > c.maxCommitVersion {
				c.maxCommitVersion = r.version
			}
		case recTypeChunkCheckpoint:
			if r, derr := decodeChunkCheckpoint(payload); derr == nil {
				c.chunkSessions[r.session] = struct{}{}
			}
		}
	}
	return c, nil
}

// deleteReclaimableSegments 删除“已被完成检查点覆盖、可安全回收”的段。
//
// 输入 keepSessions：重启恢复所依赖的检查点会话集合（最新 + 次新两份已完成
// 检查点；以及任何进行中会话，以防其装配起点被删）。
//
// 一个段可删，当且仅当同时满足：
//  1. 不是活动段；
//  2. 不含任何需要保留的检查点会话分片（keepSessions 之外的老会话残留可删）；
//  3. 段内最大提交版本 <= prevFloor：这些提交已被“次新完成检查点”完整覆盖，
//     从该检查点可连续重放到最新。
//
// prevFloor==0（只有一份检查点）时不删含数据的段：第一代先落地，
// 第二代起才真正回收，始终保留两代以支持删除中途崩溃后回退。
func (w *wal) deleteReclaimableSegments(prevFloor uint64, keepSessions map[uint64]struct{}) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	names, err := listWalSegments(w.dir)
	if err != nil {
		return 0, err
	}
	active := w.seq
	removed := 0
	for _, name := range names {
		s := parseWalSeq(name)
		if s >= active {
			continue // 绝不删除活动段及之后
		}
		c, ierr := inspectSegmentContents(filepath.Join(w.dir, name))
		if ierr != nil {
			return removed, ierr
		}
		keepChunk := false
		for sess := range c.chunkSessions {
			if _, ok := keepSessions[sess]; ok {
				keepChunk = true
				break
			}
		}
		if keepChunk {
			continue
		}
		if prevFloor > 0 && (c.maxCommitVersion == 0 || c.maxCommitVersion <= prevFloor) {
			if err := os.Remove(filepath.Join(w.dir, name)); err != nil {
				return removed, fmt.Errorf("storage: remove segment %s: %w", name, err)
			}
			removed++
		}
	}
	if removed > 0 {
		if err := syncDir(w.dir); err != nil {
			return removed, err
		}
	}
	return removed, nil
}

// deleteSegmentsBefore 删除所有序号小于 seq 的段文件（旧的整段检查点
// GC 路径使用）。必须在 wal.mu 下执行。
func (w *wal) deleteSegmentsBefore(seq int) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	names, err := listWalSegments(w.dir)
	if err != nil {
		return 0, err
	}
	// 绝不删除当前活动段及其后的段（防御性兜底）。
	active := w.seq
	removed := 0
	for _, name := range names {
		s := parseWalSeq(name)
		if s < seq && s < active {
			if err := os.Remove(filepath.Join(w.dir, name)); err != nil {
				return removed, fmt.Errorf("storage: remove segment %s: %w", name, err)
			}
			removed++
		}
	}
	if removed > 0 {
		if err := syncDir(w.dir); err != nil {
			return removed, err
		}
	}
	return removed, nil
}

func (w *wal) close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	err := w.f.Sync()
	if cerr := w.f.Close(); err == nil {
		err = cerr
	}
	w.f = nil
	return err
}

// rotateLocked 关闭当前段并创建下一段；调用方必须持有 w.mu。
func (w *wal) rotateLocked() error {
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("storage: fsync before rotate: %w", err)
	}
	if err := w.f.Close(); err != nil {
		return fmt.Errorf("storage: close full segment: %w", err)
	}
	w.seq++
	path := filepath.Join(w.dir, walName(w.seq))
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("storage: create segment %s: %w", walName(w.seq), err)
	}
	w.f = f
	w.offset = 0
	if err := syncDir(w.dir); err != nil {
		return err
	}
	return nil
}

func encodeFrame(payload []byte) []byte {
	frame := make([]byte, walHdrSize+len(payload)+walCrcSize)
	copy(frame[0:4], walMagic)
	binary.BigEndian.PutUint32(frame[4:8], uint32(len(payload)))
	copy(frame[walHdrSize:walHdrSize+len(payload)], payload)
	crc := crc32.ChecksumIEEE(frame[:walHdrSize+len(payload)])
	binary.BigEndian.PutUint32(frame[len(frame)-4:], crc)
	return frame
}

// ---------------- 记录级编解码 ----------------

func encodeCommit(rec commitRec) []byte {
	size := 1 + 8 + 4
	for _, w := range rec.writes {
		size += 1 + 4 + len(w.key) + 4 + len(w.value)
	}
	buf := make([]byte, 0, size)
	buf = append(buf, recTypeCommit)
	buf = binary.BigEndian.AppendUint64(buf, rec.version)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(rec.writes)))
	for _, w := range rec.writes {
		var flags byte
		if w.deleted {
			flags = 1
		}
		buf = append(buf, flags)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(w.key)))
		buf = append(buf, w.key...)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(w.value)))
		buf = append(buf, w.value...)
	}
	return buf
}

func decodeCommit(buf []byte) (commitRec, error) {
	r := newByteReader(buf)
	typ, err := r.byte()
	if err != nil || typ != recTypeCommit {
		return commitRec{}, io.ErrUnexpectedEOF
	}
	ver, err := r.uint64()
	if err != nil {
		return commitRec{}, err
	}
	cnt, err := r.uint32()
	if err != nil {
		return commitRec{}, err
	}
	rec := commitRec{version: ver, writes: make([]writeEntry, 0, cnt)}
	for i := uint32(0); i < cnt; i++ {
		flags, err := r.byte()
		if err != nil {
			return commitRec{}, err
		}
		key, err := r.bytes32()
		if err != nil {
			return commitRec{}, err
		}
		val, err := r.bytes32()
		if err != nil {
			return commitRec{}, err
		}
		rec.writes = append(rec.writes, writeEntry{
			key: string(key), value: val, deleted: flags&1 != 0,
		})
	}
	return rec, nil
}

func encodeCheckpoint(cp checkpointRec) []byte {
	keys := make([]string, 0, len(cp.state))
	for k := range cp.state {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	size := 1 + 8 + 4
	for _, k := range keys {
		size += 4 + len(k) + 4 + len(cp.state[k])
	}
	buf := make([]byte, 0, size)
	buf = append(buf, recTypeCheckpoint)
	buf = binary.BigEndian.AppendUint64(buf, cp.version)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(keys)))
	for _, k := range keys {
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(k)))
		buf = append(buf, k...)
		v := cp.state[k]
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(v)))
		buf = append(buf, v...)
	}
	return buf
}

func decodeCheckpoint(buf []byte) (*checkpointRec, error) {
	r := newByteReader(buf)
	typ, err := r.byte()
	if err != nil || typ != recTypeCheckpoint {
		return nil, io.ErrUnexpectedEOF
	}
	ver, err := r.uint64()
	if err != nil {
		return nil, err
	}
	cnt, err := r.uint32()
	if err != nil {
		return nil, err
	}
	cp := &checkpointRec{version: ver, state: make(map[string][]byte, cnt)}
	for i := uint32(0); i < cnt; i++ {
		k, err := r.bytes32()
		if err != nil {
			return nil, err
		}
		v, err := r.bytes32()
		if err != nil {
			return nil, err
		}
		cp.state[string(k)] = v
	}
	return cp, nil
}

func encodeChunkCheckpoint(rec chunkCheckpointRec) []byte {
	flags := byte(0)
	if rec.first {
		flags |= cpFlagFirst
	}
	if rec.final {
		flags |= cpFlagFinal
	}

	size := 1 + 8 + 8 + 4 + 1 + 4
	for _, w := range rec.entries {
		size += 4 + len(w.key) + 4 + len(w.value)
	}
	buf := make([]byte, 0, size)
	buf = append(buf, recTypeChunkCheckpoint)
	buf = binary.BigEndian.AppendUint64(buf, rec.session)
	buf = binary.BigEndian.AppendUint64(buf, rec.version)
	buf = binary.BigEndian.AppendUint32(buf, rec.seq)
	buf = append(buf, flags)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(rec.entries)))
	for _, w := range rec.entries {
		var f byte
		if w.deleted {
			f = 1
		}
		buf = append(buf, f)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(w.key)))
		buf = append(buf, w.key...)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(w.value)))
		buf = append(buf, w.value...)
	}
	return buf
}

func decodeChunkCheckpoint(buf []byte) (chunkCheckpointRec, error) {
	r := newByteReader(buf)
	typ, err := r.byte()
	if err != nil || typ != recTypeChunkCheckpoint {
		return chunkCheckpointRec{}, io.ErrUnexpectedEOF
	}
	session, err := r.uint64()
	if err != nil {
		return chunkCheckpointRec{}, err
	}
	ver, err := r.uint64()
	if err != nil {
		return chunkCheckpointRec{}, err
	}
	seq, err := r.uint32()
	if err != nil {
		return chunkCheckpointRec{}, err
	}
	flags, err := r.byte()
	if err != nil {
		return chunkCheckpointRec{}, err
	}
	cnt, err := r.uint32()
	if err != nil {
		return chunkCheckpointRec{}, err
	}
	rec := chunkCheckpointRec{
		session: session,
		version: ver,
		seq:     seq,
		first:   flags&cpFlagFirst != 0,
		final:   flags&cpFlagFinal != 0,
		entries: make([]writeEntry, 0, cnt),
	}
	for i := uint32(0); i < cnt; i++ {
		f, err := r.byte()
		if err != nil {
			return chunkCheckpointRec{}, err
		}
		k, err := r.bytes32()
		if err != nil {
			return chunkCheckpointRec{}, err
		}
		v, err := r.bytes32()
		if err != nil {
			return chunkCheckpointRec{}, err
		}
		rec.entries = append(rec.entries, writeEntry{
			key: string(k), value: v, deleted: f&1 != 0,
		})
	}
	return rec, nil
}

// byteReader 是日志解码用的顺序读取器，越界统一返回 io.ErrUnexpectedEOF。
type byteReader struct {
	buf []byte
	pos int
}

func newByteReader(buf []byte) *byteReader { return &byteReader{buf: buf} }

func (r *byteReader) byte() (byte, error) {
	if r.pos+1 > len(r.buf) {
		return 0, io.ErrUnexpectedEOF
	}
	v := r.buf[r.pos]
	r.pos++
	return v, nil
}

func (r *byteReader) uint32() (uint32, error) {
	if r.pos+4 > len(r.buf) {
		return 0, io.ErrUnexpectedEOF
	}
	v := binary.BigEndian.Uint32(r.buf[r.pos:])
	r.pos += 4
	return v, nil
}

func (r *byteReader) uint64() (uint64, error) {
	if r.pos+8 > len(r.buf) {
		return 0, io.ErrUnexpectedEOF
	}
	v := binary.BigEndian.Uint64(r.buf[r.pos:])
	r.pos += 8
	return v, nil
}

func (r *byteReader) bytes32() ([]byte, error) {
	n, err := r.uint32()
	if err != nil {
		return nil, err
	}
	if r.pos+int(n) > len(r.buf) {
		return nil, io.ErrUnexpectedEOF
	}
	out := make([]byte, n)
	copy(out, r.buf[r.pos:r.pos+int(n)])
	r.pos += int(n)
	return out, nil
}

// ---------------- 段文件工具 ----------------

func listWalSegments(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("storage: list wal dir: %w", err)
	}
	var names []string
	for _, e := range entries {
		name := e.Name()
		if !e.IsDir() && strings.HasPrefix(name, "wal-") && strings.HasSuffix(name, ".log") {
			names = append(names, name)
		}
	}
	sort.Slice(names, func(i, j int) bool {
		return parseWalSeq(names[i]) < parseWalSeq(names[j])
	})
	return names, nil
}

func parseWalSeq(name string) int {
	var seq int
	_, err := fmt.Sscanf(name, "wal-%d.log", &seq)
	if err != nil {
		return -1
	}
	return seq
}

func truncateFile(path string, size int64) error {
	f, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("storage: open %s for truncation: %w", path, err)
	}
	defer f.Close()
	if err := f.Truncate(size); err != nil {
		return fmt.Errorf("storage: truncate torn tail: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("storage: fsync after truncation: %w", err)
	}
	return nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("storage: open dir for fsync: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("storage: fsync dir: %w", err)
	}
	return nil
}
