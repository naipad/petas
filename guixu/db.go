package guixu

import (
	"bufio"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/klauspost/compress/zstd"
)

var (
	ErrNotFound = errors.New("guixu: not found")
	ErrReadOnly = errors.New("guixu: read only")
)

// KV is the core storage engine implementing a log-structured key-value store.
// It uses an append-only value log with in-memory index for fast lookups.
type KV struct {
	mu   sync.RWMutex
	opts Options

	manifest Manifest
	metrics  metricSet

	index map[string]indexEntry
	vlog  map[uint64]*os.File
	hint  *os.File
	hbw   *bufio.Writer
	cache sync.Map

	readBufPool sync.Pool

	gcSched    *scheduler
	cmpSched   *scheduler
	closed     bool
	nextSeq    uint64
	nextOff    uint64
	activeSeg  uint64
	writesN    int
	hintN      int
	cacheN     atomic.Int64
	cacheBytes atomic.Int64

	zstdEncPool *sync.Pool
	zstdDecPool *sync.Pool
}

// cacheEntry represents a cached value with metadata for validation.
type cacheEntry struct {
	segment  uint64
	offset   uint64
	expireAt int64
	value    []byte
}

// Open opens or creates a KV database at the specified directory.
// It replays the manifest and hint files to rebuild the in-memory index.
func Open(dir string, opts Options) (*KV, error) {
	opts.Dir = dir
	opts = opts.withDefaults()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	m, ok, err := readManifest(dir)
	if err != nil {
		return nil, err
	}
	if !ok {
		m = Manifest{
			Version:      2,
			Epoch:        1,
			ReadOnly:     opts.ReadOnly,
			ActiveVLog:   1,
			VLogSegments: []uint64{1},
		}
		if err := writeManifest(dir, m); err != nil {
			return nil, err
		}
	}
	m, err = replayEdits(dir, m)
	if err != nil {
		return nil, err
	}
	if err := writeManifest(dir, m); err != nil {
		return nil, err
	}

	db := &KV{
		opts:      opts,
		manifest:  m,
		metrics:   newMetricSet(),
		index:     make(map[string]indexEntry),
		vlog:      make(map[uint64]*os.File),
		activeSeg: m.ActiveVLog,
		nextSeq:   m.Seq,
		readBufPool: sync.Pool{
			New: func() interface{} {
				buf := make([]byte, 8192)
				return &buf
			},
		},
		zstdEncPool: &sync.Pool{
			New: func() interface{} {
				enc, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(opts.ZstdLevel)))
				return enc
			},
		},
		zstdDecPool: &sync.Pool{
			New: func() interface{} {
				dec, _ := zstd.NewReader(nil)
				return dec
			},
		},
	}
	if err := db.openFilesLocked(); err != nil {
		return nil, err
	}
	if err := db.replayHintLocked(); err != nil {
		_ = db.closeFilesLocked()
		return nil, err
	}
	if opts.ValidateOnOpen {
		db.validateIndexLocked()
	}
	if opts.EnableBackgroundGC {
		db.gcSched = startScheduler(opts.GCInterval, db.backgroundGC)
	}
	if opts.EnableBackgroundCompaction {
		db.cmpSched = startScheduler(opts.CompactionInterval, db.backgroundCompaction)
	}
	return db, nil
}

// Close flushes all pending writes and closes the database.
// It stops background schedulers and syncs all open files.
func (db *KV) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil
	}
	db.closed = true
	if db.gcSched != nil {
		db.gcSched.Stop()
	}
	if db.cmpSched != nil {
		db.cmpSched.Stop()
	}
	err1 := db.closeFilesLocked()
	err2 := writeManifest(db.opts.Dir, db.manifest)
	if err1 != nil {
		return err1
	}
	return err2
}

func (db *KV) Put(key, value []byte) error {
	return db.putWithTTL(key, value, 0, true)
}

func (db *KV) PutWithTTL(key, value []byte, ttl time.Duration) error {
	var exp int64
	if ttl > 0 {
		exp = time.Now().Add(ttl).UnixNano()
	}
	return db.putWithTTL(key, value, exp, true)
}

func (db *KV) putWithTTL(key, value []byte, expireAt int64, countMetrics bool) error {
	start := time.Now()
	defer func() {
		lat := uint64(time.Since(start).Nanoseconds())
		inc(&db.metrics.writeLatencyNs, lat)
		idx := atomic.AddUint64(&db.metrics.writeIdx, 1) % 1024
		atomic.StoreUint64(&db.metrics.writeLatencies[idx], lat)
	}()
	if len(key) == 0 {
		return errors.New("guixu: empty key")
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	if db.manifest.ReadOnly {
		return ErrReadOnly
	}
	return db.putWithTTLLockedTS(key, value, expireAt, countMetrics, start)
}

func (db *KV) putWithTTLLockedTS(key, value []byte, expireAt int64, countMetrics bool, ts time.Time) error {
	k := bytesToString(key)
	stored, codec := db.compressValue(value)
	rec := encodeVLogRecord(key, stored, expireAt, codec, 0)
	if err := db.rotateIfNeededLocked(len(rec)); err != nil {
		return err
	}
	off := db.nextOff
	f := db.vlog[db.activeSeg]
	n, err := f.WriteAt(rec, int64(off))
	if err != nil {
		return err
	}
	if n != len(rec) {
		return io.ErrShortWrite
	}
	db.nextOff += uint64(len(rec))
	db.writesN++
	if db.opts.VLogFsyncEvery > 0 && db.writesN%db.opts.VLogFsyncEvery == 0 {
		if err := f.Sync(); err != nil {
			return err
		}
	}
	crc := crc32.ChecksumIEEE(stored)
	vptr := VPtr{
		Version:   VPtrVersion,
		Codec:     codec,
		SegmentID: db.activeSeg,
		Offset:    off,
		Length:    uint32(len(rec)),
		Checksum:  crc,
	}
	ent := indexEntry{
		VPtr:       vptr,
		ExpireAt:   expireAt,
		DataOffset: uint32(24 + len(key)),
		DataLength: uint32(len(stored)),
	}
	db.index[k] = ent
	if err := db.appendHintLocked(k, ent); err != nil {
		return err
	}
	if countMetrics {
		inc(&db.metrics.writes, 1)
		inc(&db.metrics.writeBytes, uint64(len(value)))
		inc(&db.metrics.vlogBytes, uint64(len(rec)))
	}
	return nil
}

// Get retrieves the value for the given key.
// Returns ErrNotFound if the key doesn't exist or has expired.
func (db *KV) Get(key []byte) ([]byte, error) {
	start := time.Now()
	defer func() {
		lat := uint64(time.Since(start).Nanoseconds())
		inc(&db.metrics.readLatencyNs, lat)
		idx := atomic.AddUint64(&db.metrics.readIdx, 1) % 1024
		atomic.StoreUint64(&db.metrics.readLatencies[idx], lat)
	}()

	k := bytesToString(key)
	db.mu.RLock()
	ent, ok := db.index[k]
	if !ok || ent.Tombstone() {
		db.mu.RUnlock()
		return nil, ErrNotFound
	}
	if ent.ExpireAt > 0 && ent.ExpireAt <= time.Now().UnixNano() {
		db.mu.RUnlock()
		_ = db.Delete(key)
		return nil, ErrNotFound
	}
	if v, ok := db.cacheGet(k, ent); ok {
		db.mu.RUnlock()
		inc(&db.metrics.reads, 1)
		inc(&db.metrics.readBytes, uint64(len(v)))
		return v, nil
	}
	f := db.vlog[ent.SegmentID]
	raw, err := db.readValueLocked(f, ent)
	db.mu.RUnlock()
	if err != nil {
		return nil, ErrNotFound
	}
	out, err := db.decompressValue(raw, ent.Codec)
	if err != nil {
		return nil, err
	}
	db.cacheSet(k, ent, out)
	inc(&db.metrics.reads, 1)
	inc(&db.metrics.readBytes, uint64(len(out)))
	return out, nil
}

// GetBatch retrieves multiple values in a single read lock.
// Returns a slice where nil entries indicate missing keys.
func (db *KV) GetBatch(keys []string) ([][]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	now := time.Now().UnixNano()
	results := make([][]byte, len(keys))

	for i, k := range keys {
		ent, ok := db.index[k]
		if !ok || ent.Tombstone() || (ent.ExpireAt > 0 && ent.ExpireAt <= now) {
			continue
		}
		if v, ok := db.cacheGet(k, ent); ok {
			results[i] = v
			continue
		}
		f := db.vlog[ent.SegmentID]
		raw, err := db.readValueLocked(f, ent)
		if err != nil {
			continue
		}
		out, err := db.decompressValue(raw, ent.Codec)
		if err != nil {
			continue
		}
		db.cacheSet(k, ent, out)
		results[i] = out
	}
	return results, nil
}

func (db *KV) Delete(key []byte) error {
	k := bytesToString(key)
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.manifest.ReadOnly {
		return ErrReadOnly
	}
	if len(key) == 0 {
		return errors.New("guixu: empty key")
	}
	ent := indexEntry{
		VPtr: VPtr{
			Version: VPtrVersion,
			Flags:   vptrFlagTombstone,
		},
	}
	db.index[k] = ent
	db.cacheDelete(k)
	if err := db.appendHintLocked(k, ent); err != nil {
		return err
	}
	inc(&db.metrics.deletes, 1)
	return nil
}

func (db *KV) Merge(key, value []byte, mergeFn func(oldValue, newValue []byte) []byte) error {
	old, err := db.Get(key)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	var merged []byte
	if mergeFn != nil {
		merged = mergeFn(old, value)
	} else {
		merged = make([]byte, 0, len(old)+len(value))
		merged = append(merged, old...)
		merged = append(merged, value...)
	}
	return db.Put(key, merged)
}

func (db *KV) Has(key []byte) bool {
	db.mu.RLock()
	defer db.mu.RUnlock()
	ent, ok := db.index[bytesToString(key)]
	if !ok || ent.Tombstone() {
		return false
	}
	return ent.ExpireAt <= 0 || ent.ExpireAt > time.Now().UnixNano()
}

func (db *KV) DeleteRange(start, limit []byte) int {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.manifest.ReadOnly {
		return 0
	}
	keys := make([]string, 0, len(db.index))
	for k, v := range db.index {
		if v.Tombstone() {
			continue
		}
		if len(start) > 0 && k < string(start) {
			continue
		}
		if len(limit) > 0 && k >= string(limit) {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	n := 0
	for _, k := range keys {
		ent := indexEntry{VPtr: VPtr{Version: VPtrVersion, Flags: vptrFlagTombstone}}
		db.index[k] = ent
		db.cacheDelete(k)
		if err := db.appendHintLocked(k, ent); err != nil {
			continue
		}
		n++
	}
	atomic.AddUint64(&db.metrics.deletes, uint64(n))
	return n
}

func (db *KV) ScanPrefix(prefix, upperBound []byte, limit int, fn func(k []byte) bool) int {
	db.mu.RLock()
	keys := make([]string, 0, len(db.index))
	for k, v := range db.index {
		if v.Tombstone() || (v.ExpireAt > 0 && v.ExpireAt <= time.Now().UnixNano()) {
			continue
		}
		if len(prefix) > 0 && len(k) >= len(prefix) && k[:len(prefix)] != string(prefix) {
			continue
		}
		if len(prefix) > 0 && len(k) < len(prefix) {
			continue
		}
		if len(upperBound) > 0 && k >= string(upperBound) {
			continue
		}
		keys = append(keys, k)
	}
	db.mu.RUnlock()
	sort.Strings(keys)
	n := 0
	for _, k := range keys {
		if limit > 0 && n >= limit {
			break
		}
		n++
		if !fn([]byte(k)) {
			break
		}
	}
	return n
}

func (db *KV) Metrics() Metrics { return db.metrics.snapshot() }

func (db *KV) backgroundGC() {
	if err := db.RunGC(); err != nil {
		inc(&db.metrics.gcErrors, 1)
	}
	inc(&db.metrics.gcRuns, 1)
}

func (db *KV) backgroundCompaction() {
	// WiscKey core has no LSM compaction in this phase, keep counter for observability.
	inc(&db.metrics.compactions, 1)
}

func (db *KV) RunGC() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.opts.GCMinSegments > 0 && len(db.manifest.VLogSegments) <= db.opts.GCMinSegments {
		return nil
	}
	for _, segID := range db.manifest.VLogSegments {
		if segID == db.activeSeg {
			continue
		}
		if err := db.gcSegmentLocked(segID); err != nil {
			return err
		}
	}
	return nil
}

func (db *KV) CompactAll() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.forceRotateLockedWithPrealloc(false); err != nil {
		return err
	}
	for _, segID := range db.manifest.VLogSegments {
		if segID == db.activeSeg {
			continue
		}
		if err := db.gcSegmentLocked(segID); err != nil {
			return err
		}
	}
	return nil
}

func (db *KV) openFilesLocked() error {
	for _, segID := range db.manifest.VLogSegments {
		p := vlogPath(db.opts.Dir, segID)
		f, err := os.OpenFile(p, os.O_CREATE|os.O_RDWR, 0o644)
		if err != nil {
			return err
		}
		db.vlog[segID] = f
	}
	if db.vlog[db.activeSeg] == nil {
		f, err := os.OpenFile(vlogPath(db.opts.Dir, db.activeSeg), os.O_CREATE|os.O_RDWR, 0o644)
		if err != nil {
			return err
		}
		db.vlog[db.activeSeg] = f
	}
	st, err := db.vlog[db.activeSeg].Stat()
	if err != nil {
		return err
	}
	db.nextOff = uint64(st.Size())
	h, err := os.OpenFile(hintPath(db.opts.Dir), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	db.hint = h
	db.hbw = bufio.NewWriterSize(h, 1<<20)
	return nil
}

func (db *KV) closeFilesLocked() error {
	var first error
	for _, f := range db.vlog {
		if f == nil {
			continue
		}
		if err := f.Close(); err != nil && first == nil {
			first = err
		}
	}
	if db.hint != nil {
		if db.hbw != nil {
			if err := db.hbw.Flush(); err != nil && first == nil {
				first = err
			}
		}
		if err := db.hint.Sync(); err != nil && first == nil {
			first = err
		}
		if err := db.hint.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (db *KV) replayHintLocked() error {
	f, err := os.OpenFile(hintPath(db.opts.Dir), os.O_CREATE|os.O_RDONLY, 0o644)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	for {
		ent, err := decodeHintRecord(f)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return err
		}
		db.index[ent.Key] = ent.Entry
	}
}

func (db *KV) appendHintLocked(key string, ent indexEntry) error {
	rec := encodeHintRecord(key, ent)
	if _, err := db.hbw.Write(rec); err != nil {
		return err
	}
	db.hintN++
	if db.opts.VLogFsyncEvery <= 1 || db.hintN >= db.opts.VLogFsyncEvery {
		return db.flushHintLocked()
	}
	return nil
}

func (db *KV) flushHintLocked() error {
	if db.hbw == nil || db.hint == nil {
		return nil
	}
	if err := db.hbw.Flush(); err != nil {
		return err
	}
	db.hintN = 0
	return db.hint.Sync()
}

func (db *KV) rotateIfNeededLocked(nextRecord int) error {
	if db.opts.VLogSegmentBytes <= 0 {
		return nil
	}
	if db.nextOff+uint64(nextRecord) <= uint64(db.opts.VLogSegmentBytes) {
		return nil
	}
	return db.forceRotateLocked()
}

func (db *KV) forceRotateLocked() error {
	return db.forceRotateLockedWithPrealloc(true)
}

func (db *KV) forceRotateLockedWithPrealloc(preallocate bool) error {
	db.activeSeg++
	db.nextOff = 0
	db.manifest.Seq++
	edit := ManifestEdit{ID: db.manifest.Seq, Type: EditRotateVLog, VLogID: db.activeSeg}
	if err := appendEdit(db.opts.Dir, edit); err != nil {
		return err
	}
	applyEdit(&db.manifest, edit)
	f, err := os.OpenFile(vlogPath(db.opts.Dir, db.activeSeg), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	if preallocate && db.opts.PreallocateSegment && db.opts.VLogSegmentBytes > 0 {
		if err := f.Truncate(db.opts.VLogSegmentBytes); err != nil {
			_ = f.Close()
			return err
		}
	}
	db.vlog[db.activeSeg] = f
	return writeManifest(db.opts.Dir, db.manifest)
}

func (db *KV) readRecordLocked(f *os.File, p VPtr) (vlogRecord, error) {
	buf := make([]byte, p.Length)
	n, err := f.ReadAt(buf, int64(p.Offset))
	if err != nil && !errors.Is(err, io.EOF) {
		return vlogRecord{}, err
	}
	if n != len(buf) {
		return vlogRecord{}, io.ErrUnexpectedEOF
	}
	return decodeVLogRecordNoCRC(buf)
}

func (db *KV) readValueLocked(f *os.File, ent indexEntry) ([]byte, error) {
	if ent.DataLength > 0 {
		bufPtr := db.readBufPool.Get().(*[]byte)
		defer db.readBufPool.Put(bufPtr)

		var buf []byte
		if int(ent.DataLength) <= len(*bufPtr) {
			buf = (*bufPtr)[:ent.DataLength]
		} else {
			buf = make([]byte, ent.DataLength)
		}

		off := ent.Offset + uint64(ent.DataOffset)
		n, err := f.ReadAt(buf, int64(off))
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
		if n != len(buf) {
			return nil, io.ErrUnexpectedEOF
		}
		result := make([]byte, len(buf))
		copy(result, buf)
		return result, nil
	}
	rec, err := db.readRecordLocked(f, ent.VPtr)
	if err != nil {
		return nil, err
	}
	return rec.Value, nil
}

func (db *KV) validateIndexLocked() {
	for k, ent := range db.index {
		if ent.Tombstone() {
			continue
		}
		f := db.vlog[ent.SegmentID]
		if f == nil {
			db.index[k] = indexEntry{VPtr: VPtr{Version: VPtrVersion, Flags: vptrFlagTombstone}}
			continue
		}
		rec, err := db.readRecordLocked(f, ent.VPtr)
		if err != nil || string(rec.Key) != k {
			db.index[k] = indexEntry{VPtr: VPtr{Version: VPtrVersion, Flags: vptrFlagTombstone}}
		}
	}
}

func (db *KV) gcSegmentLocked(segID uint64) error {
	f := db.vlog[segID]
	if f == nil {
		return nil
	}
	keys := make([]string, 0)
	for k, v := range db.index {
		if v.Tombstone() {
			continue
		}
		if v.SegmentID == segID {
			keys = append(keys, k)
		}
	}
	if len(keys) == 0 {
		_ = f.Close()
		delete(db.vlog, segID)
		_ = os.Remove(vlogPath(db.opts.Dir, segID))
		return nil
	}
	for _, k := range keys {
		val, err := db.getUnlocked([]byte(k))
		if err != nil {
			continue
		}
		oldSeg := db.index[k].SegmentID
		if oldSeg != segID {
			continue
		}
		if err := db.putWithTTLLockedTS([]byte(k), val, db.index[k].ExpireAt, false, time.Now()); err != nil {
			return err
		}
		rateLimitSleep(int64(len(val)), db.opts.GCMaxWriteBytesPS)
	}
	_ = f.Close()
	delete(db.vlog, segID)
	_ = os.Remove(vlogPath(db.opts.Dir, segID))
	return nil
}

func (db *KV) getUnlocked(key []byte) ([]byte, error) {
	ent, ok := db.index[bytesToString(key)]
	if !ok || ent.Tombstone() {
		return nil, ErrNotFound
	}
	f := db.vlog[ent.SegmentID]
	if f == nil {
		return nil, ErrNotFound
	}
	raw, err := db.readValueLocked(f, ent)
	if err != nil {
		return nil, err
	}
	return db.decompressValue(raw, ent.Codec)
}

type indexEntry struct {
	VPtr
	ExpireAt   int64
	DataOffset uint32
	DataLength uint32
}

func (e indexEntry) Tombstone() bool { return e.VPtr.Tombstone() }

type vlogRecord struct {
	Key      []byte
	Value    []byte
	ExpireAt int64
	Codec    byte
	Flags    byte
}

func vlogPath(dir string, segID uint64) string {
	return filepath.Join(dir, "vlog-"+padSeg(segID)+".dat")
}

func hintPath(dir string) string { return filepath.Join(dir, "HINT.v2.bin") }

func padSeg(v uint64) string { return fmt.Sprintf("%06d", v) }

func bytesToString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}

// cacheGet retrieves a value from the in-memory cache.
// Returns (value, true) if found and valid, (nil, false) otherwise.
func (db *KV) cacheGet(k string, ent indexEntry) ([]byte, bool) {
	if !db.opts.EnableValueCache {
		return nil, false
	}
	raw, ok := db.cache.Load(k)
	if !ok {
		return nil, false
	}
	ce, ok := raw.(cacheEntry)
	if !ok {
		return nil, false
	}
	if ce.segment != ent.SegmentID || ce.offset != ent.Offset || ce.expireAt != ent.ExpireAt {
		return nil, false
	}
	return ce.value, true
}

// cacheSet stores a value in the cache with random eviction when full.
func (db *KV) cacheSet(k string, ent indexEntry, v []byte) {
	if !db.opts.EnableValueCache || db.opts.ValueCacheMaxItems <= 0 {
		return
	}
	size := int64(len(v))
	if old, ok := db.cache.Load(k); ok {
		oldEntry, _ := old.(cacheEntry)
		delta := size - int64(len(oldEntry.value))
		if db.opts.ValueCacheMaxBytes > 0 && delta > 0 && db.cacheBytes.Load()+delta > db.opts.ValueCacheMaxBytes {
			return
		}
		db.cache.Store(k, cacheEntry{
			segment:  ent.SegmentID,
			offset:   ent.Offset,
			expireAt: ent.ExpireAt,
			value:    v,
		})
		if delta != 0 {
			db.cacheBytes.Add(delta)
		}
		return
	}
	n := int(db.cacheN.Load())
	if n >= db.opts.ValueCacheMaxItems || (db.opts.ValueCacheMaxBytes > 0 && db.cacheBytes.Load()+size > db.opts.ValueCacheMaxBytes) {
		if n > db.opts.ValueCacheMaxItems/2 {
			db.evictRandom()
		} else {
			return
		}
	}
	_, loaded := db.cache.LoadOrStore(k, cacheEntry{
		segment:  ent.SegmentID,
		offset:   ent.Offset,
		expireAt: ent.ExpireAt,
		value:    v,
	})
	if !loaded {
		db.cacheN.Add(1)
		db.cacheBytes.Add(size)
	}
}

// evictRandom removes up to 8 random entries from the cache.
func (db *KV) evictRandom() {
	count := 0
	db.cache.Range(func(key, value interface{}) bool {
		if count >= 8 {
			return false
		}
		db.cacheDelete(key.(string))
		count++
		return true
	})
}

func (db *KV) cacheDelete(k string) {
	if !db.opts.EnableValueCache {
		return
	}
	if old, loaded := db.cache.LoadAndDelete(k); loaded {
		if ce, ok := old.(cacheEntry); ok {
			db.cacheBytes.Add(-int64(len(ce.value)))
		}
		db.cacheN.Add(-1)
	}
}
