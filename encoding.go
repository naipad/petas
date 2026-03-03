package petas

import (
	"encoding/binary"
	"math"
	"strings"
)

const (
	sep = ":"

	prefixString  = "s"
	prefixHash    = "h"
	prefixZMember = "zs"
	prefixZIndex  = "zi"
	prefixBucket  = "b"
)

func validateToken(v string) error {
	if v == "" {
		return ErrEmptyKey
	}
	if strings.Contains(v, sep) {
		return ErrInvalidToken
	}
	return nil
}

func validateBucket(bucket string) error {
	if bucket == "" {
		return ErrInvalidBucket
	}
	return validateToken(bucket)
}

func encodeVersion(v uint64) string {
	var out [20]byte
	for i := range out {
		out[i] = '0'
	}
	i := 19
	for v > 0 && i >= 0 {
		out[i] = byte('0' + (v % 10))
		v /= 10
		i--
	}
	return string(out[:])
}

func appendVersion(out []byte, v uint64) []byte {
	var ver [20]byte
	for i := range ver {
		ver[i] = '0'
	}
	i := len(ver) - 1
	for v > 0 && i >= 0 {
		ver[i] = byte('0' + (v % 10))
		v /= 10
		i--
	}
	return append(out, ver[:]...)
}

func bucketVersionKey(bucket string) []byte {
	out := make([]byte, 0, len(prefixBucket)+1+len(bucket))
	out = append(out, prefixBucket...)
	out = append(out, ':')
	out = append(out, bucket...)
	return out
}

func strKey(bucket string, version uint64, key string) []byte {
	out := make([]byte, 0, len(prefixString)+1+len(bucket)+1+20+1+len(key))
	out = append(out, prefixString...)
	out = append(out, ':')
	out = append(out, bucket...)
	out = append(out, ':')
	out = appendVersion(out, version)
	out = append(out, ':')
	out = append(out, key...)
	return out
}

func hashFieldKey(bucket string, version uint64, hkey, field string) []byte {
	out := make([]byte, 0, len(prefixHash)+1+len(bucket)+1+20+1+len(hkey)+1+len(field))
	out = append(out, prefixHash...)
	out = append(out, ':')
	out = append(out, bucket...)
	out = append(out, ':')
	out = appendVersion(out, version)
	out = append(out, ':')
	out = append(out, hkey...)
	out = append(out, ':')
	out = append(out, field...)
	return out
}

func hashPrefix(bucket string, version uint64, hkey string) []byte {
	out := make([]byte, 0, len(prefixHash)+1+len(bucket)+1+20+1+len(hkey)+1)
	out = append(out, prefixHash...)
	out = append(out, ':')
	out = append(out, bucket...)
	out = append(out, ':')
	out = appendVersion(out, version)
	out = append(out, ':')
	out = append(out, hkey...)
	out = append(out, ':')
	return out
}

func zMemberKey(bucket string, version uint64, zkey, member string) []byte {
	out := make([]byte, 0, len(prefixZMember)+1+len(bucket)+1+20+1+len(zkey)+1+len(member))
	out = append(out, prefixZMember...)
	out = append(out, ':')
	out = append(out, bucket...)
	out = append(out, ':')
	out = appendVersion(out, version)
	out = append(out, ':')
	out = append(out, zkey...)
	out = append(out, ':')
	out = append(out, member...)
	return out
}

func zMemberPrefix(bucket string, version uint64, zkey string) []byte {
	out := make([]byte, 0, len(prefixZMember)+1+len(bucket)+1+20+1+len(zkey)+1)
	out = append(out, prefixZMember...)
	out = append(out, ':')
	out = append(out, bucket...)
	out = append(out, ':')
	out = appendVersion(out, version)
	out = append(out, ':')
	out = append(out, zkey...)
	out = append(out, ':')
	return out
}

func zIndexKey(bucket string, version uint64, zkey string, score float64, member string) []byte {
	s := encodeFloat64Order(score)
	buf := make([]byte, 0, len(prefixZIndex)+len(bucket)+len(zkey)+len(member)+64)
	buf = append(buf, prefixZIndex...)
	buf = append(buf, ':')
	buf = append(buf, bucket...)
	buf = append(buf, ':')
	buf = appendVersion(buf, version)
	buf = append(buf, ':')
	buf = append(buf, zkey...)
	buf = append(buf, ':')
	buf = append(buf, s[:]...)
	buf = append(buf, ':')
	buf = append(buf, member...)
	return buf
}

func zIndexPrefix(bucket string, version uint64, zkey string) []byte {
	out := make([]byte, 0, len(prefixZIndex)+1+len(bucket)+1+20+1+len(zkey)+1)
	out = append(out, prefixZIndex...)
	out = append(out, ':')
	out = append(out, bucket...)
	out = append(out, ':')
	out = appendVersion(out, version)
	out = append(out, ':')
	out = append(out, zkey...)
	out = append(out, ':')
	return out
}

func zCountKey(bucket string, version uint64, zkey string) []byte {
	out := make([]byte, 0, len(prefixZMember)+7+len(bucket)+1+20+1+len(zkey))
	out = append(out, prefixZMember...)
	out = append(out, ':', 'c', 'o', 'u', 'n', 't', ':')
	out = append(out, bucket...)
	out = append(out, ':')
	out = appendVersion(out, version)
	out = append(out, ':')
	out = append(out, zkey...)
	return out
}

func zTTLFlagKey(bucket string, version uint64, zkey string) []byte {
	out := make([]byte, 0, len(prefixZMember)+5+len(bucket)+1+20+1+len(zkey))
	out = append(out, prefixZMember...)
	out = append(out, ':', 't', 't', 'l', ':')
	out = append(out, bucket...)
	out = append(out, ':')
	out = appendVersion(out, version)
	out = append(out, ':')
	out = append(out, zkey...)
	return out
}

func hCountKey(bucket string, version uint64, hkey string) []byte {
	out := make([]byte, 0, len(prefixHash)+7+len(bucket)+1+20+1+len(hkey))
	out = append(out, prefixHash...)
	out = append(out, ':', 'c', 'o', 'u', 'n', 't', ':')
	out = append(out, bucket...)
	out = append(out, ':')
	out = appendVersion(out, version)
	out = append(out, ':')
	out = append(out, hkey...)
	return out
}

func hTTLFlagKey(bucket string, version uint64, hkey string) []byte {
	out := make([]byte, 0, len(prefixHash)+5+len(bucket)+1+20+1+len(hkey))
	out = append(out, prefixHash...)
	out = append(out, ':', 't', 't', 'l', ':')
	out = append(out, bucket...)
	out = append(out, ':')
	out = appendVersion(out, version)
	out = append(out, ':')
	out = append(out, hkey...)
	return out
}

func encodeFloat64Order(f float64) [8]byte {
	u := math.Float64bits(f)
	if u&(1<<63) != 0 {
		u = ^u
	} else {
		u ^= 1 << 63
	}
	var out [8]byte
	binary.BigEndian.PutUint64(out[:], u)
	return out
}

func decodeFloat64Order(raw []byte) (float64, bool) {
	if len(raw) != 8 {
		return 0, false
	}
	u := binary.BigEndian.Uint64(raw)
	if u&(1<<63) != 0 {
		u ^= 1 << 63
	} else {
		u = ^u
	}
	return math.Float64frombits(u), true
}

func parseZMemberSliceFromIndex(key []byte, prefix []byte) ([]byte, bool) {
	if len(key) <= len(prefix)+9 {
		return nil, false
	}
	if key[len(prefix)+8] != ':' {
		return nil, false
	}
	return key[len(prefix)+9:], true
}

func parseZScoreFromIndex(key []byte, prefix []byte) (float64, bool) {
	if len(key) <= len(prefix)+8 {
		return 0, false
	}
	return decodeFloat64Order(key[len(prefix) : len(prefix)+8])
}
