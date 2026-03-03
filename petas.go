package petas

import (
	"sync"
	"sync/atomic"

	"github.com/naipad/petas/guixu"
)

// DB is the main petas database handle providing high-level KV operations.
// It wraps the storage engine and provides semantic data types (String, Hash, ZSet).
type DB struct {
	lv   *engineDB
	opts Options

	versionMu sync.Mutex
	cacheMu   sync.RWMutex
	versions  map[string]uint64

	keyLocksMu sync.Mutex
	keyLocks   map[string]*refMutex

	defaultBucketVer atomic.Uint64

	cleanupCh   chan []byte
	cleanupStop chan struct{}
	cleanupDone chan struct{}
}

func Open(path string, opts *Options) (*DB, error) {
	o := opts.withDefaults()
	if err := o.validate(); err != nil {
		return nil, err
	}
	kvCompression := guixu.CompressionNone
	switch o.Compression {
	case CompressionS2:
		kvCompression = guixu.CompressionS2
	case CompressionZSTD:
		kvCompression = guixu.CompressionZSTD
	case CompressionAuto:
		kvCompression = guixu.CompressionAuto
	default:
		kvCompression = guixu.CompressionNone
	}
	lv, err := openEngine(path, guixu.Options{
		Compression:                kvCompression,
		CompressMinSize:            o.CompressMin,
		AutoZstdMinSize:            o.AutoZstdMin,
		EnableBackgroundGC:         o.KVAutoGC == nil || *o.KVAutoGC,
		GCInterval:                 o.KVGCInterval,
		GCMinStaleRatio:            o.KVGCMinStaleRatio,
		GCMinSegments:              o.KVGCMinSegments,
		VLogSegmentBytes:           o.KVSegmentMaxSizeBytes,
		VLogFsyncEvery:             o.KVSyncEveryWrites,
		PreallocateSegment:         o.KVPreallocateSegment == nil || *o.KVPreallocateSegment,
		EnableBackgroundCompaction: false,
		EnableValueCache:           o.KVEnableValueCache == nil || *o.KVEnableValueCache,
		ValueCacheMaxItems:         o.KVValueCacheMaxItems,
		ValueCacheMaxBytes:         o.KVValueCacheMaxBytes,
	})
	if err != nil {
		return nil, err
	}
	db := &DB{
		lv:          lv,
		opts:        o,
		versions:    make(map[string]uint64),
		keyLocks:    make(map[string]*refMutex),
		cleanupCh:   make(chan []byte, 4096),
		cleanupStop: make(chan struct{}),
		cleanupDone: make(chan struct{}),
	}
	db.startCleanupWorker()
	return db, nil
}

func (db *DB) Close() error {
	db.stopCleanupWorker()
	return db.lv.Close()
}

func (db *DB) Compact(minStaleRatio float64) error {
	return db.lv.RunValueLogGC(minStaleRatio)
}

func (db *DB) resolveBucket(bucket string) string {
	if bucket != "" {
		return bucket
	}
	return db.opts.DefaultBucket
}
