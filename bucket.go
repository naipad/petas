package petas

import (
	"errors"
	"strconv"

	"github.com/naipad/petas/guixu"
)

func (db *DB) bucketVersion(bucket string) (uint64, error) {
	if err := validateBucket(bucket); err != nil {
		return 0, err
	}
	if bucket == db.opts.DefaultBucket {
		if v := db.defaultBucketVer.Load(); v > 0 {
			return v, nil
		}
	}
	db.cacheMu.RLock()
	if v, ok := db.versions[bucket]; ok && v > 0 {
		db.cacheMu.RUnlock()
		if bucket == db.opts.DefaultBucket {
			db.defaultBucketVer.Store(v)
		}
		return v, nil
	}
	db.cacheMu.RUnlock()

	db.versionMu.Lock()
	defer db.versionMu.Unlock()
	if v, ok := db.versions[bucket]; ok && v > 0 {
		return v, nil
	}

	key := bucketVersionKey(bucket)
	v, err := db.lv.engineGet(key)
	if errors.Is(err, guixu.ErrNotFound) {
		var b guixu.Batch
		b.Put(key, []byte("1"))
		if putErr := db.lv.engineWrite(&b); putErr != nil {
			return 0, putErr
		}
		db.cacheMu.Lock()
		db.versions[bucket] = 1
		db.cacheMu.Unlock()
		if bucket == db.opts.DefaultBucket {
			db.defaultBucketVer.Store(1)
		}
		return 1, nil
	}
	if err != nil {
		return 0, err
	}
	n, convErr := strconv.ParseUint(string(v), 10, 64)
	if convErr != nil || n == 0 {
		var b guixu.Batch
		b.Put(key, []byte("1"))
		_ = db.lv.engineWrite(&b)
		db.cacheMu.Lock()
		db.versions[bucket] = 1
		db.cacheMu.Unlock()
		if bucket == db.opts.DefaultBucket {
			db.defaultBucketVer.Store(1)
		}
		return 1, nil
	}
	db.cacheMu.Lock()
	db.versions[bucket] = n
	db.cacheMu.Unlock()
	if bucket == db.opts.DefaultBucket {
		db.defaultBucketVer.Store(n)
	}
	return n, nil
}

func (db *DB) DeleteBucket(bucket string) error {
	db.versionMu.Lock()
	defer db.versionMu.Unlock()

	if err := validateBucket(bucket); err != nil {
		return err
	}
	key := bucketVersionKey(bucket)

	ver := uint64(1)
	if v, err := db.lv.engineGet(key); err == nil {
		if n, convErr := strconv.ParseUint(string(v), 10, 64); convErr == nil && n > 0 {
			ver = n
		}
	} else if !errors.Is(err, guixu.ErrNotFound) {
		return err
	}

	var b guixu.Batch
	next := ver + 1
	b.Put(key, []byte(strconv.FormatUint(next, 10)))
	if err := db.lv.engineWrite(&b); err != nil {
		return err
	}
	db.cacheMu.Lock()
	db.versions[bucket] = next
	db.cacheMu.Unlock()
	if bucket == db.opts.DefaultBucket {
		db.defaultBucketVer.Store(next)
	}
	return nil
}

func (db *DB) BucketVersion(bucket string) (uint64, error) {
	ver, err := db.bucketVersion(bucket)
	return ver, err
}
