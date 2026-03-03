package petas

import (
	"errors"
	"time"

	"github.com/naipad/petas/guixu"
)

func (db *DB) SetString(bucket, key string, value []byte, ttl time.Duration) error {
	bucket = db.resolveBucket(bucket)
	ver, err := db.bucketVersion(bucket)
	if err != nil {
		return err
	}
	if err = validateToken(key); err != nil {
		return err
	}
	return db.lv.Put(strKey(bucket, ver, key), db.encodeValue(value, ttl), nil)
}

func (db *DB) GetString(bucket, key string) ([]byte, bool, error) {
	bucket = db.resolveBucket(bucket)
	ver, err := db.bucketVersion(bucket)
	if err != nil {
		return nil, false, err
	}
	if err = validateToken(key); err != nil {
		return nil, false, err
	}
	k := strKey(bucket, ver, key)
	raw, err := db.lv.Get(k, nil)
	if errors.Is(err, guixu.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	payload, expireAt, ok, derr := db.decodeValue(raw)
	if derr != nil || !ok {
		db.enqueueCleanupDelete(k)
		return nil, false, nil
	}
	if isExpired(expireAt, time.Now().Unix()) {
		db.enqueueCleanupDelete(k)
		return nil, false, nil
	}
	return payload, true, nil
}

func (db *DB) DelString(bucket, key string) error {
	bucket = db.resolveBucket(bucket)
	ver, err := db.bucketVersion(bucket)
	if err != nil {
		return err
	}
	if err = validateToken(key); err != nil {
		return err
	}
	return db.lv.Delete(strKey(bucket, ver, key), nil)
}
