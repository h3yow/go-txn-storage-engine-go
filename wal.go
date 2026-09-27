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

	recTypeCommit     = 1
	recTypeCheckpoint = 2
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
func (w *wal) readAllRecords() (commits []commitRec, cp *checkpointRec, stats recoveryStats, err error) {
	names, err := listWalSegments(w.dir)
	if err != nil {
		return nil, nil, stats, err
	}
	stats.SegmentsScanned = len(names)
	for _, name := range names {
		data, rerr := os.ReadFile(filepath.Join(w.dir, name))
		if rerr != nil {
			return nil, nil, stats, rerr
		}
		pos := int64(0)
		for pos < int64(len(data)) {
			payload, frameLen, ok := readFrame(data[pos:])
			if !ok {
				// openWAL 已截断残尾，这里再遇到只能是非尾部损坏。
				return nil, nil, stats, fmt.Errorf("%w: unexpected bad frame in %s at offset %d",
					ErrLogCorrupted, name, pos)
			}
			pos += int64(frameLen)
			switch payload[0] {
			case recTypeCommit:
				rec, cerr := decodeCommit(payload)
				if cerr != nil {
					return nil, nil, stats, fmt.Errorf("%w: %v in %s at offset %d",
						ErrLogCorrupted, cerr, name, pos)
				}
				commits = append(commits, rec)
				stats.CommitRecords++
			case recTypeCheckpoint:
				c, cerr := decodeCheckpoint(payload)
				if cerr != nil {
					return nil, nil, stats, fmt.Errorf("%w: %v in %s at offset %d",
						ErrLogCorrupted, cerr, name, pos)
				}
				cp = c // 后出现的检查点覆盖先出现的（正常至多一个）
				stats.CheckpointFound = true
				stats.CheckpointVer = c.version
			default:
				return nil, nil, stats, fmt.Errorf("%w: unknown record type %d in %s",
					ErrLogCorrupted, payload[0], name)
			}
		}
	}
	return commits, cp, stats, nil
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

// activeSeq 返回当前活动段序号（GC 据此知道哪些段比它老）。
func (w *wal) activeSeq() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.seq
}

// deleteSegmentsBefore 删除所有序号小于 seq 的段文件。
func (w *wal) deleteSegmentsBefore(seq int) (int, error) {
	names, err := listWalSegments(w.dir)
	if err != nil {
		return 0, err
	}
	removed := 0
	for _, name := range names {
		if parseWalSeq(name) < seq {
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
