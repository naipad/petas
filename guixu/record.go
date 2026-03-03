package guixu

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

// vlog record format:
// [crc32][klen][vlen][expireAt][flags][codec][rsv2][key][value]
func encodeVLogRecord(key, value []byte, expireAt int64, codec byte, flags byte) []byte {
	hdr := 4 + 4 + 4 + 8 + 1 + 1 + 2
	b := make([]byte, hdr+len(key)+len(value))
	binary.BigEndian.PutUint32(b[4:8], uint32(len(key)))
	binary.BigEndian.PutUint32(b[8:12], uint32(len(value)))
	binary.BigEndian.PutUint64(b[12:20], uint64(expireAt))
	b[20] = flags
	b[21] = codec
	copy(b[24:], key)
	copy(b[24+len(key):], value)
	crc := crc32.ChecksumIEEE(b[4:])
	binary.BigEndian.PutUint32(b[:4], crc)
	return b
}

func decodeVLogRecord(b []byte) (vlogRecord, error) {
	if len(b) < 24 {
		return vlogRecord{}, io.ErrUnexpectedEOF
	}
	got := binary.BigEndian.Uint32(b[:4])
	if got != crc32.ChecksumIEEE(b[4:]) {
		return vlogRecord{}, errors.New("guixu: vlog crc mismatch")
	}
	klen := int(binary.BigEndian.Uint32(b[4:8]))
	vlen := int(binary.BigEndian.Uint32(b[8:12]))
	exp := int64(binary.BigEndian.Uint64(b[12:20]))
	flags := b[20]
	codec := b[21]
	if 24+klen+vlen > len(b) {
		return vlogRecord{}, io.ErrUnexpectedEOF
	}
	key := b[24 : 24+klen]
	val := b[24+klen : 24+klen+vlen]
	return vlogRecord{
		Key:      key,
		Value:    val,
		ExpireAt: exp,
		Codec:    codec,
		Flags:    flags,
	}, nil
}

// decodeVLogRecordNoCRC is the read hot-path variant.
// Integrity is validated by startup checks and explicit Validate().
func decodeVLogRecordNoCRC(b []byte) (vlogRecord, error) {
	if len(b) < 24 {
		return vlogRecord{}, io.ErrUnexpectedEOF
	}
	klen := int(binary.BigEndian.Uint32(b[4:8]))
	vlen := int(binary.BigEndian.Uint32(b[8:12]))
	exp := int64(binary.BigEndian.Uint64(b[12:20]))
	flags := b[20]
	codec := b[21]
	if 24+klen+vlen > len(b) {
		return vlogRecord{}, io.ErrUnexpectedEOF
	}
	key := b[24 : 24+klen]
	val := b[24+klen : 24+klen+vlen]
	return vlogRecord{
		Key:      key,
		Value:    val,
		ExpireAt: exp,
		Codec:    codec,
		Flags:    flags,
	}, nil
}

type hintRecord struct {
	Key   string
	Entry indexEntry
}

// hint record format:
// [crc32][klen][expireAt][dataOff][dataLen][vptr(28)][key]
func encodeHintRecord(key string, ent indexEntry) []byte {
	vb := ent.VPtr.Encode()
	hdr := 4 + 4 + 8 + 4 + 4 + len(vb)
	b := make([]byte, hdr+len(key))
	binary.BigEndian.PutUint32(b[4:8], uint32(len(key)))
	binary.BigEndian.PutUint64(b[8:16], uint64(ent.ExpireAt))
	binary.BigEndian.PutUint32(b[16:20], ent.DataOffset)
	binary.BigEndian.PutUint32(b[20:24], ent.DataLength)
	copy(b[24:24+len(vb)], vb)
	copy(b[24+len(vb):], []byte(key))
	crc := crc32.ChecksumIEEE(b[4:])
	binary.BigEndian.PutUint32(b[:4], crc)
	return b
}

func decodeHintRecord(r io.Reader) (hintRecord, error) {
	var hdr [52]byte // 4+4+8+4+4+28
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return hintRecord{}, err
	}
	klen := int(binary.BigEndian.Uint32(hdr[4:8]))
	exp := int64(binary.BigEndian.Uint64(hdr[8:16]))
	dataOff := binary.BigEndian.Uint32(hdr[16:20])
	dataLen := binary.BigEndian.Uint32(hdr[20:24])
	vptr, err := DecodeVPtr(hdr[24:52])
	if err != nil {
		return hintRecord{}, err
	}
	key := make([]byte, klen)
	if _, err := io.ReadFull(r, key); err != nil {
		return hintRecord{}, err
	}
	all := append(hdr[4:], key...)
	if binary.BigEndian.Uint32(hdr[:4]) != crc32.ChecksumIEEE(all) {
		return hintRecord{}, errors.New("guixu: hint crc mismatch")
	}
	return hintRecord{
		Key: string(key),
		Entry: indexEntry{
			VPtr:       vptr,
			ExpireAt:   exp,
			DataOffset: dataOff,
			DataLength: dataLen,
		},
	}, nil
}
