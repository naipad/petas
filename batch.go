package petas

import (
	"sync"
	"time"

	"github.com/naipad/petas/guixu"
)

type Batch struct {
	db     *DB
	batch  guixu.Batch
	closed bool
	mu     sync.Mutex

	versions map[string]uint64
	zstate   map[string]batchZMemberState
}

type batchZMemberState struct {
	present bool
	score   float64
}

func (db *DB) NewBatch() *Batch {
	return &Batch{
		db:       db,
		versions: make(map[string]uint64),
		zstate:   make(map[string]batchZMemberState),
	}
}

func (b *Batch) ensureOpen() error {
	if b.closed {
		return ErrBatchClosed
	}
	return nil
}

func (b *Batch) SetString(bucket, key string, value []byte, ttl time.Duration) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.ensureOpen(); err != nil {
		return err
	}
	ver, bucket, err := b.resolveBucketVersion(bucket)
	if err != nil {
		return err
	}
	if err = validateToken(key); err != nil {
		return err
	}
	b.batch.Put(strKey(bucket, ver, key), b.db.encodeValue(value, ttl))
	return nil
}

func (b *Batch) DelString(bucket, key string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.ensureOpen(); err != nil {
		return err
	}
	ver, bucket, err := b.resolveBucketVersion(bucket)
	if err != nil {
		return err
	}
	if err = validateToken(key); err != nil {
		return err
	}
	b.batch.Delete(strKey(bucket, ver, key))
	return nil
}

func (b *Batch) HSet(bucket, hkey, field string, value []byte, ttl time.Duration) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.ensureOpen(); err != nil {
		return err
	}
	ver, bucket, err := b.resolveBucketVersion(bucket)
	if err != nil {
		return err
	}
	if err = validateToken(hkey); err != nil {
		return err
	}
	if err = validateToken(field); err != nil {
		return err
	}
	b.batch.Put(hashFieldKey(bucket, ver, hkey, field), b.db.encodeValue(value, ttl))
	b.batch.Delete(hCountKey(bucket, ver, hkey))
	if ttl > 0 {
		b.batch.Put(hTTLFlagKey(bucket, ver, hkey), []byte{1})
	}
	return nil
}

func (b *Batch) HDel(bucket, hkey, field string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.ensureOpen(); err != nil {
		return err
	}
	ver, bucket, err := b.resolveBucketVersion(bucket)
	if err != nil {
		return err
	}
	if err = validateToken(hkey); err != nil {
		return err
	}
	if err = validateToken(field); err != nil {
		return err
	}
	b.batch.Delete(hashFieldKey(bucket, ver, hkey, field))
	b.batch.Delete(hCountKey(bucket, ver, hkey))
	return nil
}

func (b *Batch) HMSet(bucket, hkey string, fields map[string][]byte, ttl time.Duration) error {
	for field, value := range fields {
		if err := b.HSet(bucket, hkey, field, value, ttl); err != nil {
			return err
		}
	}
	return nil
}

func (b *Batch) HMDel(bucket, hkey string, fields ...string) error {
	for _, field := range fields {
		if err := b.HDel(bucket, hkey, field); err != nil {
			return err
		}
	}
	return nil
}

func (b *Batch) ZAdd(bucket, zkey string, score float64, member string, ttl time.Duration) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.ensureOpen(); err != nil {
		return err
	}
	ver, bucket, err := b.resolveBucketVersion(bucket)
	if err != nil {
		return err
	}
	if err = validateToken(zkey); err != nil {
		return err
	}
	if err = validateToken(member); err != nil {
		return err
	}
	memberK := zMemberKey(bucket, ver, zkey, member)
	stateKey := batchZStateKey(bucket, ver, zkey, member)

	if st, ok := b.zstate[stateKey]; ok && st.present {
		b.batch.Delete(zIndexKey(bucket, ver, zkey, st.score, member))
	} else if !ok {
		old, getErr := b.db.lv.Get(memberK, nil)
		if getErr == nil {
			if oldScore, okV, stale, derr := decodeZScoreValue(old, time.Now().Unix()); derr == nil && okV && !stale {
				b.batch.Delete(zIndexKey(bucket, ver, zkey, oldScore, member))
			}
		}
	}

	b.batch.Put(memberK, encodeZScoreValue(score, ttl))
	b.batch.Put(zIndexKey(bucket, ver, zkey, score, member), encodeZIndexValue(ttl))
	b.batch.Delete(zCountKey(bucket, ver, zkey))
	if ttl > 0 {
		b.batch.Put(zTTLFlagKey(bucket, ver, zkey), []byte{1})
	}
	b.zstate[stateKey] = batchZMemberState{present: true, score: score}
	return nil
}

func (b *Batch) ZDel(bucket, zkey, member string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.ensureOpen(); err != nil {
		return err
	}
	ver, bucket, err := b.resolveBucketVersion(bucket)
	if err != nil {
		return err
	}
	if err = validateToken(zkey); err != nil {
		return err
	}
	if err = validateToken(member); err != nil {
		return err
	}
	memberK := zMemberKey(bucket, ver, zkey, member)
	stateKey := batchZStateKey(bucket, ver, zkey, member)

	if st, ok := b.zstate[stateKey]; ok {
		if st.present {
			b.batch.Delete(zIndexKey(bucket, ver, zkey, st.score, member))
		}
	} else {
		old, getErr := b.db.lv.Get(memberK, nil)
		if getErr == nil {
			if oldScore, okV, stale, derr := decodeZScoreValue(old, time.Now().Unix()); derr == nil && (okV || stale) {
				b.batch.Delete(zIndexKey(bucket, ver, zkey, oldScore, member))
			}
		}
	}
	b.batch.Delete(memberK)
	b.batch.Delete(zCountKey(bucket, ver, zkey))
	b.zstate[stateKey] = batchZMemberState{present: false}
	return nil
}

func (b *Batch) ZMSet(bucket, zkey string, members map[string]float64, ttl time.Duration) error {
	for member, score := range members {
		if err := b.ZAdd(bucket, zkey, score, member, ttl); err != nil {
			return err
		}
	}
	return nil
}

func (b *Batch) ZMDel(bucket, zkey string, members ...string) error {
	for _, member := range members {
		if err := b.ZDel(bucket, zkey, member); err != nil {
			return err
		}
	}
	return nil
}

func (b *Batch) resolveBucketVersion(bucket string) (uint64, string, error) {
	bucket = b.db.resolveBucket(bucket)
	if ver, ok := b.versions[bucket]; ok {
		return ver, bucket, nil
	}
	ver, err := b.db.bucketVersion(bucket)
	if err != nil {
		return 0, bucket, err
	}
	b.versions[bucket] = ver
	return ver, bucket, nil
}

func batchZStateKey(bucket string, ver uint64, zkey, member string) string {
	return bucket + ":" + encodeVersion(ver) + ":" + zkey + ":" + member
}

func (b *Batch) Commit() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.ensureOpen(); err != nil {
		return err
	}
	b.closed = true
	return b.db.lv.Write(&b.batch, nil)
}

func (b *Batch) Abort() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.ensureOpen(); err != nil {
		return err
	}
	b.closed = true
	b.batch.Reset()
	return nil
}

func (b *Batch) Closed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closed
}
