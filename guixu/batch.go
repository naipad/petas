package guixu

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"time"
)

type WriteOptions struct {
	Sync bool
}

type ReadOptions struct{}

type Range struct {
	Start []byte
	Limit []byte
}

type Batch struct {
	db  *KV
	ops []batchOp
}

type batchOp struct {
	key      []byte
	value    []byte
	expireAt int64
	del      bool
}

func (db *KV) NewBatch() *Batch {
	return &Batch{db: db, ops: make([]batchOp, 0, 16)}
}

func (b *Batch) Put(key, value []byte) {
	b.ops = append(b.ops, batchOp{key: append([]byte(nil), key...), value: append([]byte(nil), value...)})
}

func (b *Batch) PutWithTTL(key, value []byte, ttl time.Duration) {
	var exp int64
	if ttl > 0 {
		exp = time.Now().Add(ttl).UnixNano()
	}
	b.ops = append(b.ops, batchOp{key: append([]byte(nil), key...), value: append([]byte(nil), value...), expireAt: exp})
}

func (b *Batch) Delete(key []byte) {
	b.ops = append(b.ops, batchOp{key: append([]byte(nil), key...), del: true})
}

func (b *Batch) Len() int { return len(b.ops) }

func (b *Batch) SetDB(db *KV) { b.db = db }

func (b *Batch) Reset() { b.ops = b.ops[:0] }

// Dump encodes batch operations into a portable binary frame.
func (b *Batch) Dump() []byte {
	var out bytes.Buffer
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(b.ops)))
	out.Write(hdr[:])
	for _, op := range b.ops {
		var h [21]byte // del(1)+exp(8)+klen(4)+vlen(4)+crc(4)
		if op.del {
			h[0] = 1
		}
		binary.BigEndian.PutUint64(h[1:9], uint64(op.expireAt))
		binary.BigEndian.PutUint32(h[9:13], uint32(len(op.key)))
		binary.BigEndian.PutUint32(h[13:17], uint32(len(op.value)))
		c := crc32.ChecksumIEEE(op.key)
		c = crc32.Update(c, crc32.IEEETable, op.value)
		binary.BigEndian.PutUint32(h[17:21], c)
		out.Write(h[:])
		out.Write(op.key)
		out.Write(op.value)
	}
	return out.Bytes()
}

// Load decodes a batch frame produced by Dump.
func (b *Batch) Load(in []byte) error {
	rd := bytes.NewReader(in)
	var nbuf [4]byte
	if _, err := io.ReadFull(rd, nbuf[:]); err != nil {
		return err
	}
	n := int(binary.BigEndian.Uint32(nbuf[:]))
	b.ops = b.ops[:0]
	for i := 0; i < n; i++ {
		var h [21]byte
		if _, err := io.ReadFull(rd, h[:]); err != nil {
			return err
		}
		del := h[0] == 1
		exp := int64(binary.BigEndian.Uint64(h[1:9]))
		klen := int(binary.BigEndian.Uint32(h[9:13]))
		vlen := int(binary.BigEndian.Uint32(h[13:17]))
		wantCRC := binary.BigEndian.Uint32(h[17:21])
		if klen < 0 || vlen < 0 || klen > 1<<24 || vlen > 1<<28 {
			return errors.New("guixu: invalid batch frame")
		}
		k := make([]byte, klen)
		v := make([]byte, vlen)
		if _, err := io.ReadFull(rd, k); err != nil {
			return err
		}
		if _, err := io.ReadFull(rd, v); err != nil {
			return err
		}
		c := crc32.ChecksumIEEE(k)
		c = crc32.Update(c, crc32.IEEETable, v)
		if c != wantCRC {
			return errors.New("guixu: batch crc mismatch")
		}
		b.ops = append(b.ops, batchOp{key: k, value: v, expireAt: exp, del: del})
	}
	return nil
}

// Replay is an alias of Commit for explicit semantic usage.
func (b *Batch) Replay(opts WriteOptions) error {
	return b.Commit(opts)
}

func (b *Batch) Commit(opts WriteOptions) error {
	db := b.db
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.manifest.ReadOnly {
		return ErrReadOnly
	}
	for _, op := range b.ops {
		if op.del {
			ent := indexEntry{VPtr: VPtr{Version: VPtrVersion, Flags: vptrFlagTombstone}}
			db.index[string(op.key)] = ent
			if err := db.appendHintLocked(string(op.key), ent); err != nil {
				return err
			}
			inc(&db.metrics.deletes, 1)
			continue
		}
		stored, codec := db.compressValue(op.value)
		rec := encodeVLogRecord(op.key, stored, op.expireAt, codec, 0)
		if err := db.rotateIfNeededLocked(len(rec)); err != nil {
			return err
		}
		off := db.nextOff
		f := db.vlog[db.activeSeg]
		if _, err := f.WriteAt(rec, int64(off)); err != nil {
			return err
		}
		db.nextOff += uint64(len(rec))
		crc := crc32.ChecksumIEEE(stored)
		ent := indexEntry{VPtr: VPtr{Version: VPtrVersion, Codec: codec, SegmentID: db.activeSeg, Offset: off, Length: uint32(len(rec)), Checksum: crc}, ExpireAt: op.expireAt}
		db.index[string(op.key)] = ent
		if err := db.appendHintLocked(string(op.key), ent); err != nil {
			return err
		}
		inc(&db.metrics.writes, 1)
		inc(&db.metrics.writeBytes, uint64(len(op.value)))
		inc(&db.metrics.vlogBytes, uint64(len(rec)))
	}
	if opts.Sync {
		if f := db.vlog[db.activeSeg]; f != nil {
			if err := f.Sync(); err != nil {
				return err
			}
		}
		if db.hint != nil {
			if err := db.hint.Sync(); err != nil {
				return err
			}
		}
	}
	return nil
}
