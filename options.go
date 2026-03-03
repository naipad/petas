package petas

import (
	"fmt"
	"time"
)

type CompressionCodec string

const (
	CompressionNone CompressionCodec = "none"
	CompressionZSTD CompressionCodec = "zstd"
	CompressionS2   CompressionCodec = "s2"
	CompressionAuto CompressionCodec = "auto"
)

// Options configures the behavior of a petas database instance.
type Options struct {
	DefaultBucket string
	Compression   CompressionCodec
	CompressMin   int
	AutoZstdMin   int

	KVAutoGC              *bool
	KVGCInterval          time.Duration
	KVGCMinStaleRatio     float64
	KVGCMinSegments       int
	KVSegmentMaxSizeBytes int64
	KVPreallocateSegment  *bool
	KVSyncEveryWrites     int
	KVEnableValueCache    *bool
	KVValueCacheMaxItems  int
	KVValueCacheMaxBytes  int64
}

func (o *Options) withDefaults() Options {
	autoGC := true
	prealloc := true
	valueCache := true
	out := Options{
		DefaultBucket:         "default",
		Compression:           CompressionAuto,
		CompressMin:           256,
		AutoZstdMin:           16 * 1024,
		KVAutoGC:              &autoGC,
		KVGCInterval:          60 * time.Second,
		KVGCMinStaleRatio:     0.30,
		KVGCMinSegments:       4,
		KVSegmentMaxSizeBytes: 128 << 20,
		KVPreallocateSegment:  &prealloc,
		KVSyncEveryWrites:     16,
		KVEnableValueCache:    &valueCache,
		KVValueCacheMaxItems:  32768,
		KVValueCacheMaxBytes:  64 << 20,
	}
	if o == nil {
		return out
	}
	if o.DefaultBucket != "" {
		out.DefaultBucket = o.DefaultBucket
	}
	if o.Compression != "" {
		out.Compression = o.Compression
	}
	if o.CompressMin >= 0 {
		out.CompressMin = o.CompressMin
	}
	if o.AutoZstdMin > 0 {
		out.AutoZstdMin = o.AutoZstdMin
	}
	if o.KVAutoGC != nil {
		out.KVAutoGC = o.KVAutoGC
	}
	if o.KVGCInterval > 0 {
		out.KVGCInterval = o.KVGCInterval
	}
	if o.KVGCMinStaleRatio > 0 {
		out.KVGCMinStaleRatio = o.KVGCMinStaleRatio
	}
	if o.KVGCMinSegments > 0 {
		out.KVGCMinSegments = o.KVGCMinSegments
	}
	if o.KVSegmentMaxSizeBytes > 0 {
		out.KVSegmentMaxSizeBytes = o.KVSegmentMaxSizeBytes
	}
	if o.KVPreallocateSegment != nil {
		out.KVPreallocateSegment = o.KVPreallocateSegment
	}
	if o.KVSyncEveryWrites > 0 {
		out.KVSyncEveryWrites = o.KVSyncEveryWrites
	}
	if o.KVEnableValueCache != nil {
		out.KVEnableValueCache = o.KVEnableValueCache
	}
	if o.KVValueCacheMaxItems > 0 {
		out.KVValueCacheMaxItems = o.KVValueCacheMaxItems
	}
	if o.KVValueCacheMaxBytes > 0 {
		out.KVValueCacheMaxBytes = o.KVValueCacheMaxBytes
	}
	return out
}

func (o Options) validate() error {
	switch o.Compression {
	case CompressionNone, CompressionZSTD, CompressionS2, CompressionAuto:
		if o.AutoZstdMin > 0 && o.AutoZstdMin < o.CompressMin {
			return fmt.Errorf("petas: AutoZstdMin(%d) must be >= CompressMin(%d)", o.AutoZstdMin, o.CompressMin)
		}
		if o.KVGCMinStaleRatio < 0 || o.KVGCMinStaleRatio > 1 {
			return fmt.Errorf("petas: KVGCMinStaleRatio(%f) must be in [0,1]", o.KVGCMinStaleRatio)
		}
		return nil
	default:
		return fmt.Errorf("petas: unsupported compression codec: %q", o.Compression)
	}
}
