package guixu

import "time"

// CompressionMode controls vlog value compression.
type CompressionMode string

const (
	CompressionNone CompressionMode = "none"
	CompressionS2   CompressionMode = "s2"
	CompressionZSTD CompressionMode = "zstd"
	CompressionAuto CompressionMode = "auto"
)

// Options defines v0.2 engine behavior.
type Options struct {
	Dir string

	// LSM/index side
	MemtableBytes     int64
	FlushTriggerBytes int64
	MaxL0Files        int

	// Value log side
	VLogSegmentBytes   int64
	VLogFsyncEvery     int
	PreallocateSegment bool

	Compression     CompressionMode
	CompressMinSize int
	AutoZstdMinSize int
	ZstdLevel       int

	// GC/compaction
	EnableBackgroundGC bool
	GCInterval         time.Duration
	GCMinStaleRatio    float64
	GCMinSegments      int
	GCMaxWriteBytesPS  int64

	EnableBackgroundCompaction bool
	CompactionInterval         time.Duration
	CompactionMaxWriteBytesPS  int64

	// Value cache
	EnableValueCache   bool
	ValueCacheMaxItems int
	ValueCacheMaxBytes int64

	// Integrity and startup
	ValidateOnOpen bool
	ReadOnly       bool
}

func (o Options) withDefaults() Options {
	if o.MemtableBytes <= 0 {
		o.MemtableBytes = 64 << 20
	}
	if o.FlushTriggerBytes <= 0 {
		o.FlushTriggerBytes = 48 << 20
	}
	if o.MaxL0Files <= 0 {
		o.MaxL0Files = 8
	}
	if o.VLogSegmentBytes <= 0 {
		o.VLogSegmentBytes = 128 << 20
	}
	if o.VLogFsyncEvery <= 0 {
		o.VLogFsyncEvery = 16
	}
	if !o.PreallocateSegment {
		o.PreallocateSegment = true
	}
	if o.Compression == "" {
		o.Compression = CompressionAuto
	}
	if o.CompressMinSize <= 0 {
		o.CompressMinSize = 256
	}
	if o.AutoZstdMinSize <= 0 {
		o.AutoZstdMinSize = 16 << 10
	}
	if o.ZstdLevel == 0 {
		o.ZstdLevel = 1
	}
	if o.GCInterval <= 0 {
		o.GCInterval = 60 * time.Second
	}
	if o.GCMinStaleRatio <= 0 {
		o.GCMinStaleRatio = 0.30
	}
	if o.GCMinSegments <= 0 {
		o.GCMinSegments = 4
	}
	if o.CompactionInterval <= 0 {
		o.CompactionInterval = 45 * time.Second
	}
	if o.ValueCacheMaxItems <= 0 {
		o.ValueCacheMaxItems = 32768
	}
	if o.ValueCacheMaxBytes <= 0 {
		o.ValueCacheMaxBytes = 64 << 20
	}
	if !o.EnableValueCache {
		o.EnableValueCache = true
	}
	if !o.EnableBackgroundGC {
		o.EnableBackgroundGC = true
	}
	if !o.EnableBackgroundCompaction {
		o.EnableBackgroundCompaction = true
	}
	if !o.ValidateOnOpen {
		o.ValidateOnOpen = true
	}
	return o
}
