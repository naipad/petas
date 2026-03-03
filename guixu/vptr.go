package guixu

import (
	"encoding/binary"
	"errors"
)

// VPtrVersion is pointer protocol version.
const VPtrVersion byte = 1

const (
	vptrFlagTombstone = 1 << 0
)

// VPtr references a value in vlog.
// Stable on-disk format (big-endian):
// [1 ver][1 codec][1 flags][1 reserved][8 segmentID][8 offset][4 length][4 checksum]
type VPtr struct {
	Version   byte
	Codec     byte
	Flags     byte
	SegmentID uint64
	Offset    uint64
	Length    uint32
	Checksum  uint32
}

const vptrEncodedSize = 1 + 1 + 1 + 1 + 8 + 8 + 4 + 4

func (p VPtr) Tombstone() bool { return (p.Flags & vptrFlagTombstone) != 0 }

func (p VPtr) Encode() []byte {
	b := make([]byte, vptrEncodedSize)
	b[0] = VPtrVersion
	b[1] = p.Codec
	b[2] = p.Flags
	binary.BigEndian.PutUint64(b[4:12], p.SegmentID)
	binary.BigEndian.PutUint64(b[12:20], p.Offset)
	binary.BigEndian.PutUint32(b[20:24], p.Length)
	binary.BigEndian.PutUint32(b[24:28], p.Checksum)
	return b
}

func DecodeVPtr(b []byte) (VPtr, error) {
	if len(b) != vptrEncodedSize {
		return VPtr{}, errors.New("guixu: invalid vptr size")
	}
	if b[0] != VPtrVersion {
		return VPtr{}, errors.New("guixu: unsupported vptr version")
	}
	return VPtr{
		Version:   b[0],
		Codec:     b[1],
		Flags:     b[2],
		SegmentID: binary.BigEndian.Uint64(b[4:12]),
		Offset:    binary.BigEndian.Uint64(b[12:20]),
		Length:    binary.BigEndian.Uint32(b[20:24]),
		Checksum:  binary.BigEndian.Uint32(b[24:28]),
	}, nil
}
