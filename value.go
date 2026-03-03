package petas

import (
	"encoding/binary"
	"errors"
	"time"
)

// value format: [1-byte version][1-byte codec][8-byte expireAt] + payload
const valueHeaderVersion byte = 1
const valueCodecNone byte = 0

func (db *DB) encodeValue(payload []byte, ttl time.Duration) []byte {
	expireAt := int64(0)
	if ttl > 0 {
		expireAt = time.Now().Add(ttl).Unix()
	}
	encoded := payload
	out := make([]byte, 10+len(encoded))
	out[0] = valueHeaderVersion
	out[1] = valueCodecNone
	binary.BigEndian.PutUint64(out[2:10], uint64(expireAt))
	copy(out[10:], encoded)
	return out
}

func (db *DB) decodeValue(raw []byte) (payload []byte, expireAt int64, ok bool, err error) {
	if len(raw) < 10 {
		return nil, 0, false, errors.New("petas: invalid value header")
	}
	if raw[0] != valueHeaderVersion {
		return nil, 0, false, errors.New("petas: unknown value version")
	}
	codecID := raw[1]
	expireAt = int64(binary.BigEndian.Uint64(raw[2:10]))
	data := raw[10:]
	if codecID == valueCodecNone {
		return data, expireAt, true, nil
	}
	return nil, 0, false, errors.New("petas: unsupported upper-layer codec")
}

func isExpired(expireAt int64, now int64) bool {
	return expireAt > 0 && expireAt <= now
}

func decodeValueMeta(raw []byte) (expireAt int64, ok bool) {
	if len(raw) < 10 || raw[0] != valueHeaderVersion {
		return 0, false
	}
	expireAt = int64(binary.BigEndian.Uint64(raw[2:10]))
	return expireAt, true
}
