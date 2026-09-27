package engine

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
)

// wal 是仅追加的提交日志。每条记录格式：
//
//	[payloadLen uint32][crc32(payload) uint32][payload]
//
// payload 布局：
//
//	commitTS uint64 | opCount uint32 | op...
//	op: deleteFlag byte | keyLen uint32 | key | valLen uint32 | value
//
// 恢复时若末尾出现写了一半的记录（长度不够或 CRC 不符），
// 截断损坏尾部，已读到的完整记录照常生效。
type wal struct {
	f      *os.File
	offset int64 // 当前写位置（已确认完好的字节数）
}

type walRecord struct {
	commitTS uint64
	ops      []walOp
}

type walOp struct {
	delete bool
	key    string
	value  []byte
}

const walHeaderSize = 8 // len(4) + crc(4)

func openWAL(path string) (*wal, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	w := &wal{f: f}
	if err := w.truncateTornTail(); err != nil {
		f.Close()
		return nil, err
	}
	return w, nil
}

// truncateTornTail 扫描文件，把写位置定位到最后一条完整记录之后，
// 并截掉其后可能存在的损坏尾部。
func (w *wal) truncateTornTail() error {
	var off int64
	buf := make([]byte, walHeaderSize)
	for {
		if _, err := w.f.ReadAt(buf, off); err != nil {
			break // 头部都读不全：损坏尾部
		}
		payloadLen := binary.LittleEndian.Uint32(buf[:4])
		crc := binary.LittleEndian.Uint32(buf[4:8])
		payload := make([]byte, payloadLen)
		if _, err := w.f.ReadAt(payload, off+walHeaderSize); err != nil {
			break // payload 读不全：损坏尾部
		}
		if crc32.ChecksumIEEE(payload) != crc {
			break // CRC 不符：损坏尾部
		}
		off += walHeaderSize + int64(payloadLen)
	}
	if err := w.f.Truncate(off); err != nil {
		return err
	}
	if _, err := w.f.Seek(off, io.SeekStart); err != nil {
		return err
	}
	w.offset = off
	return nil
}

// append 写入一条记录并 fsync，返回前数据已落盘。
func (w *wal) append(rec walRecord) error {
	payload := encodeRecord(rec)
	buf := make([]byte, walHeaderSize+len(payload))
	binary.LittleEndian.PutUint32(buf[:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(buf[4:8], crc32.ChecksumIEEE(payload))
	copy(buf[walHeaderSize:], payload)
	if _, err := w.f.Write(buf); err != nil {
		return err
	}
	if err := w.f.Sync(); err != nil {
		return err
	}
	w.offset += int64(len(buf))
	return nil
}

// replay 顺序读出全部完整记录（openWAL 已截断损坏尾部，故这里不会遇到坏记录）。
func (w *wal) replay() ([]walRecord, error) {
	var recs []walRecord
	off := int64(0)
	hdr := make([]byte, walHeaderSize)
	for {
		if _, err := w.f.ReadAt(hdr, off); err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		payloadLen := binary.LittleEndian.Uint32(hdr[:4])
		payload := make([]byte, payloadLen)
		if _, err := w.f.ReadAt(payload, off+walHeaderSize); err != nil {
			return nil, err
		}
		rec, err := decodeRecord(payload)
		if err != nil {
			return nil, err
		}
		recs = append(recs, rec)
		off += walHeaderSize + int64(payloadLen)
	}
	return recs, nil
}

// rewrite 用给定记录整体重建日志文件（用于回收）。
// 先写临时文件再 rename，保证崩溃时旧文件仍可用。
func (w *wal) rewrite(recs []walRecord) error {
	tmpPath := w.f.Name() + ".tmp"
	tmp, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	var size int64
	for _, rec := range recs {
		payload := encodeRecord(rec)
		buf := make([]byte, walHeaderSize+len(payload))
		binary.LittleEndian.PutUint32(buf[:4], uint32(len(payload)))
		binary.LittleEndian.PutUint32(buf[4:8], crc32.ChecksumIEEE(payload))
		copy(buf[walHeaderSize:], payload)
		if _, err := tmp.Write(buf); err != nil {
			tmp.Close()
			os.Remove(tmpPath)
			return err
		}
		size += int64(len(buf))
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, w.f.Name()); err != nil {
		return err
	}
	// 重新打开新文件继续追加。
	nf, err := os.OpenFile(w.f.Name(), os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	w.f.Close()
	w.f = nf
	if _, err := w.f.Seek(size, io.SeekStart); err != nil {
		return err
	}
	w.offset = size
	return nil
}

func (w *wal) close() error { return w.f.Close() }

func encodeRecord(rec walRecord) []byte {
	size := 8 + 4
	for _, op := range rec.ops {
		size += 1 + 4 + len(op.key) + 4 + len(op.value)
	}
	buf := make([]byte, 0, size)
	buf = binary.LittleEndian.AppendUint64(buf, rec.commitTS)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(rec.ops)))
	for _, op := range rec.ops {
		var flag byte
		if op.delete {
			flag = 1
		}
		buf = append(buf, flag)
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(op.key)))
		buf = append(buf, op.key...)
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(op.value)))
		buf = append(buf, op.value...)
	}
	return buf
}

func decodeRecord(payload []byte) (walRecord, error) {
	var rec walRecord
	if len(payload) < 12 {
		return rec, io.ErrUnexpectedEOF
	}
	rec.commitTS = binary.LittleEndian.Uint64(payload[:8])
	opCount := binary.LittleEndian.Uint32(payload[8:12])
	rest := payload[12:]
	for i := uint32(0); i < opCount; i++ {
		if len(rest) < 5 {
			return rec, io.ErrUnexpectedEOF
		}
		op := walOp{delete: rest[0] == 1}
		keyLen := binary.LittleEndian.Uint32(rest[1:5])
		rest = rest[5:]
		if len(rest) < int(keyLen)+4 {
			return rec, io.ErrUnexpectedEOF
		}
		op.key = string(rest[:keyLen])
		rest = rest[keyLen:]
		valLen := binary.LittleEndian.Uint32(rest[:4])
		rest = rest[4:]
		if len(rest) < int(valLen) {
			return rec, io.ErrUnexpectedEOF
		}
		op.value = append([]byte(nil), rest[:valLen]...)
		rest = rest[valLen:]
		rec.ops = append(rec.ops, op)
	}
	return rec, nil
}
