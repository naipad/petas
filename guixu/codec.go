package guixu

import (
	"errors"

	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/zstd"
)

const (
	codecNone byte = 0
	codecS2   byte = 1
	codecZstd byte = 2
)

// compressValue compresses the value using the configured compression mode.
// Returns the compressed data and codec identifier.
func (db *KV) compressValue(v []byte) ([]byte, byte) {
	if len(v) < db.opts.CompressMinSize || db.opts.Compression == CompressionNone {
		return v, codecNone
	}
	mode := db.opts.Compression
	if mode == CompressionAuto {
		if len(v) >= db.opts.AutoZstdMinSize {
			mode = CompressionZSTD
		} else {
			mode = CompressionS2
		}
	}
	switch mode {
	case CompressionS2:
		return s2.Encode(nil, v), codecS2
	case CompressionZSTD:
		enc := db.zstdEncPool.Get().(*zstd.Encoder)
		out := enc.EncodeAll(v, make([]byte, 0, len(v)/2))
		db.zstdEncPool.Put(enc)
		return out, codecZstd
	default:
		return v, codecNone
	}
}

// decompressValue decompresses the value based on the codec identifier.
func (db *KV) decompressValue(v []byte, codec byte) ([]byte, error) {
	switch codec {
	case codecNone:
		return v, nil
	case codecS2:
		return s2.Decode(nil, v)
	case codecZstd:
		dec := db.zstdDecPool.Get().(*zstd.Decoder)
		out, err := dec.DecodeAll(v, nil)
		db.zstdDecPool.Put(dec)
		return out, err
	default:
		return nil, errors.New("guixu: unknown codec")
	}
}
