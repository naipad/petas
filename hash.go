package petas

import (
	"bytes"
	"errors"
	"strconv"
	"time"

	"github.com/naipad/petas/guixu"
)

func (db *DB) HSet(bucket, hkey, field string, value []byte, ttl time.Duration) error {
	bucket = db.resolveBucket(bucket)
	ver, err := db.bucketVersion(bucket)
	if err != nil {
		return err
	}
	if err = validateToken(hkey); err != nil {
		return err
	}
	if err = validateToken(field); err != nil {
		return err
	}
	k := hashFieldKey(bucket, ver, hkey, field)
	countK := hCountKey(bucket, ver, hkey)
	ttlFlagK := hTTLFlagKey(bucket, ver, hkey)

	var b guixu.Batch
	now := time.Now().Unix()
	existing, getErr := db.lv.Get(k, nil)
	newField := errors.Is(getErr, guixu.ErrNotFound)
	if getErr != nil && !newField {
		return getErr
	}
	if !newField {
		if expireAt, ok := decodeValueMeta(existing); !ok || isExpired(expireAt, now) {
			newField = true
		}
	}

	b.Put(k, db.encodeValue(value, ttl))
	if newField {
		cnt := db.loadHCount(bucket, ver, hkey)
		b.Put(countK, encodeZCount(cnt+1))
	}
	if ttl > 0 {
		b.Put(ttlFlagK, []byte{1})
	}
	return db.lv.Write(&b, nil)
}

func (db *DB) HGet(bucket, hkey, field string) ([]byte, bool, error) {
	bucket = db.resolveBucket(bucket)
	ver, err := db.bucketVersion(bucket)
	if err != nil {
		return nil, false, err
	}
	if err = validateToken(hkey); err != nil {
		return nil, false, err
	}
	if err = validateToken(field); err != nil {
		return nil, false, err
	}
	k := hashFieldKey(bucket, ver, hkey, field)
	raw, err := db.lv.Get(k, nil)
	if errors.Is(err, guixu.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	expireAt, metaOK := decodeValueMeta(raw)
	if !metaOK || isExpired(expireAt, time.Now().Unix()) {
		db.enqueueCleanupDelete(k)
		return nil, false, nil
	}
	payload, _, ok, derr := db.decodeValue(raw)
	if !ok || derr != nil {
		db.enqueueCleanupDelete(k)
		return nil, false, nil
	}
	return payload, true, nil
}

func (db *DB) HDel(bucket, hkey, field string) error {
	bucket = db.resolveBucket(bucket)
	ver, err := db.bucketVersion(bucket)
	if err != nil {
		return err
	}
	if err = validateToken(hkey); err != nil {
		return err
	}
	if err = validateToken(field); err != nil {
		return err
	}
	k := hashFieldKey(bucket, ver, hkey, field)
	countK := hCountKey(bucket, ver, hkey)
	var b guixu.Batch

	now := time.Now().Unix()
	existing, getErr := db.lv.Get(k, nil)
	exists := getErr == nil
	if getErr != nil && !errors.Is(getErr, guixu.ErrNotFound) {
		return getErr
	}
	if exists {
		if expireAt, ok := decodeValueMeta(existing); !ok || isExpired(expireAt, now) {
			exists = false
		}
	}

	b.Delete(k)
	if exists {
		cnt := db.loadHCount(bucket, ver, hkey)
		if cnt <= 1 {
			b.Delete(countK)
		} else {
			b.Put(countK, encodeZCount(cnt-1))
		}
	}
	return db.lv.Write(&b, nil)
}

func (db *DB) HLen(bucket, hkey string) (int, error) {
	bucket = db.resolveBucket(bucket)
	ver, err := db.bucketVersion(bucket)
	if err != nil {
		return 0, err
	}
	if err = validateToken(hkey); err != nil {
		return 0, err
	}
	if !db.hasHTTL(bucket, ver, hkey) {
		if raw, getErr := db.lv.Get(hCountKey(bucket, ver, hkey), nil); getErr == nil {
			return decodeZCount(raw), nil
		}
	}
	prefix := hashPrefix(bucket, ver, hkey)
	it := db.lv.NewIterator(prefixRange(prefix), nil)
	defer it.Release()
	count := 0
	now := time.Now().Unix()
	for ok := it.First(); ok; ok = it.Next() {
		if !bytes.HasPrefix(it.Key(), prefix) {
			break
		}
		expireAt, decOK := decodeValueMeta(it.Value())
		if !decOK || isExpired(expireAt, now) {
			continue
		}
		count++
	}
	if itErr := it.Error(); itErr != nil {
		return 0, itErr
	}
	_ = db.lv.Put(hCountKey(bucket, ver, hkey), encodeZCount(count), nil)
	return count, nil
}

func (db *DB) HGetAll(bucket, hkey string) (map[string][]byte, error) {
	bucket = db.resolveBucket(bucket)
	ver, err := db.bucketVersion(bucket)
	if err != nil {
		return nil, err
	}
	if err = validateToken(hkey); err != nil {
		return nil, err
	}
	prefix := hashPrefix(bucket, ver, hkey)
	it := db.lv.NewIterator(prefixRange(prefix), nil)
	defer it.Release()

	now := time.Now().Unix()
	initialCap := 0
	if !db.hasHTTL(bucket, ver, hkey) {
		initialCap = db.loadHCount(bucket, ver, hkey)
	}
	out := make(map[string][]byte, initialCap)
	for ok := it.First(); ok; ok = it.Next() {
		k := it.Key()
		if !bytes.HasPrefix(k, prefix) {
			break
		}
		field := string(k[len(prefix):])
		expireAt, metaOK := decodeValueMeta(it.Value())
		if !metaOK || isExpired(expireAt, now) {
			continue
		}
		payload, _, decOK, _ := db.decodeValue(it.Value())
		if !decOK {
			continue
		}
		out[field] = append([]byte(nil), payload...)
	}
	return out, it.Error()
}

func (db *DB) loadHCount(bucket string, ver uint64, hkey string) int {
	raw, err := db.lv.Get(hCountKey(bucket, ver, hkey), nil)
	if err == nil {
		return decodeZCount(raw)
	}
	prefix := hashPrefix(bucket, ver, hkey)
	it := db.lv.NewIterator(prefixRange(prefix), nil)
	defer it.Release()
	now := time.Now().Unix()
	count := 0
	for ok := it.First(); ok; ok = it.Next() {
		k := it.Key()
		if !bytes.HasPrefix(k, prefix) {
			break
		}
		expireAt, decOK := decodeValueMeta(it.Value())
		if !decOK || isExpired(expireAt, now) {
			continue
		}
		count++
	}
	if err := it.Error(); err != nil {
		return 0
	}
	_ = db.lv.Put(hCountKey(bucket, ver, hkey), encodeZCount(count), nil)
	return count
}

func (db *DB) hasHTTL(bucket string, ver uint64, hkey string) bool {
	_, err := db.lv.Get(hTTLFlagKey(bucket, ver, hkey), nil)
	return err == nil
}

type HashEntry struct {
	Field string
	Value []byte
}

func (db *DB) HHas(bucket, hkey, field string) (bool, error) {
	_, ok, err := db.HGet(bucket, hkey, field)
	return ok, err
}

func (db *DB) HMSet(bucket, hkey string, fields map[string][]byte, ttl time.Duration) error {
	if len(fields) == 0 {
		return nil
	}
	bucket = db.resolveBucket(bucket)
	ver, err := db.bucketVersion(bucket)
	if err != nil {
		return err
	}
	if err = validateToken(hkey); err != nil {
		return err
	}

	now := time.Now().Unix()
	countK := hCountKey(bucket, ver, hkey)
	ttlFlagK := hTTLFlagKey(bucket, ver, hkey)
	var b guixu.Batch
	countLoaded := false
	count := 0

	for field, value := range fields {
		if err = validateToken(field); err != nil {
			return err
		}
		k := hashFieldKey(bucket, ver, hkey, field)
		existing, getErr := db.lv.Get(k, nil)
		newField := errors.Is(getErr, guixu.ErrNotFound)
		if getErr != nil && !newField {
			return getErr
		}
		if !newField {
			if expireAt, ok := decodeValueMeta(existing); !ok || isExpired(expireAt, now) {
				newField = true
			}
		}
		if newField {
			if !countLoaded {
				count = db.loadHCount(bucket, ver, hkey)
				countLoaded = true
			}
			count++
		}
		b.Put(k, db.encodeValue(value, ttl))
	}
	if countLoaded {
		b.Put(countK, encodeZCount(count))
	}
	if ttl > 0 {
		b.Put(ttlFlagK, []byte{1})
	}
	return db.lv.Write(&b, nil)
}

func (db *DB) HMGet(bucket, hkey string, fields ...string) (map[string][]byte, error) {
	out := make(map[string][]byte, len(fields))
	err := db.HMGetEach(bucket, hkey, func(field string, value []byte) bool {
		out[field] = append([]byte(nil), value...)
		return true
	}, fields...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (db *DB) HMGetEach(bucket, hkey string, fn func(field string, value []byte) bool, fields ...string) error {
	bucket = db.resolveBucket(bucket)
	ver, err := db.bucketVersion(bucket)
	if err != nil {
		return err
	}
	if err = validateToken(hkey); err != nil {
		return err
	}
	prefix := hashPrefix(bucket, ver, hkey)
	keyBuf := make([]byte, 0, len(prefix)+64)
	now := time.Now().Unix()
	for _, field := range fields {
		if err = validateToken(field); err != nil {
			return err
		}
		keyBuf = append(keyBuf[:0], prefix...)
		keyBuf = append(keyBuf, field...)
		raw, getErr := db.lv.Get(keyBuf, nil)
		if getErr != nil {
			if getErr == guixu.ErrNotFound {
				continue
			}
			return getErr
		}
		expireAt, metaOK := decodeValueMeta(raw)
		if !metaOK || isExpired(expireAt, now) {
			db.enqueueCleanupDelete(keyBuf)
			continue
		}
		v, _, ok, derr := db.decodeValue(raw)
		if derr != nil || !ok {
			db.enqueueCleanupDelete(keyBuf)
			continue
		}
		if ok && !fn(field, v) {
			return nil
		}
	}
	return nil
}

func (db *DB) HMDel(bucket, hkey string, fields ...string) error {
	if len(fields) == 0 {
		return nil
	}
	bucket = db.resolveBucket(bucket)
	ver, err := db.bucketVersion(bucket)
	if err != nil {
		return err
	}
	if err = validateToken(hkey); err != nil {
		return err
	}
	now := time.Now().Unix()
	countK := hCountKey(bucket, ver, hkey)
	count := db.loadHCount(bucket, ver, hkey)
	removed := 0
	var b guixu.Batch

	for _, field := range fields {
		if err = validateToken(field); err != nil {
			return err
		}
		k := hashFieldKey(bucket, ver, hkey, field)
		existing, getErr := db.lv.Get(k, nil)
		exists := getErr == nil
		if getErr != nil && !errors.Is(getErr, guixu.ErrNotFound) {
			return getErr
		}
		if exists {
			if expireAt, ok := decodeValueMeta(existing); !ok || isExpired(expireAt, now) {
				exists = false
			}
		}
		if exists {
			removed++
		}
		b.Delete(k)
	}
	if removed > 0 {
		next := count - removed
		if next <= 0 {
			b.Delete(countK)
		} else {
			b.Put(countK, encodeZCount(next))
		}
	}
	return db.lv.Write(&b, nil)
}

func (db *DB) HIncr(bucket, hkey, field string, delta int64, ttl time.Duration) (int64, error) {
	bucket = db.resolveBucket(bucket)
	lockKey := "hincr:" + bucket + ":" + hkey + ":" + field
	db.lockKey(lockKey)
	defer db.unlockKey(lockKey)
	ver, err := db.bucketVersion(bucket)
	if err != nil {
		return 0, err
	}
	if err = validateToken(hkey); err != nil {
		return 0, err
	}
	if err = validateToken(field); err != nil {
		return 0, err
	}

	k := hashFieldKey(bucket, ver, hkey, field)
	countK := hCountKey(bucket, ver, hkey)
	ttlFlagK := hTTLFlagKey(bucket, ver, hkey)
	now := time.Now().Unix()
	newField := false
	cur := int64(0)
	raw, getErr := db.lv.Get(k, nil)
	if errors.Is(getErr, guixu.ErrNotFound) {
		newField = true
	} else if getErr != nil {
		return 0, getErr
	} else {
		expireAt, metaOK := decodeValueMeta(raw)
		if !metaOK || isExpired(expireAt, now) {
			newField = true
		} else {
			payload, _, ok, derr := db.decodeValue(raw)
			if derr != nil || !ok {
				newField = true
			} else {
				cur, err = strconv.ParseInt(string(payload), 10, 64)
				if err != nil {
					return 0, err
				}
			}
		}
	}
	next := cur + delta
	var b guixu.Batch
	b.Put(k, db.encodeValue([]byte(strconv.FormatInt(next, 10)), ttl))
	if newField {
		cnt := db.loadHCount(bucket, ver, hkey)
		b.Put(countK, encodeZCount(cnt+1))
	}
	if ttl > 0 {
		b.Put(ttlFlagK, []byte{1})
	}
	if err := db.lv.Write(&b, nil); err != nil {
		return 0, err
	}
	return next, nil
}

func (db *DB) HScan(bucket, hkey, start string, limit int) ([]HashEntry, error) {
	if limit <= 0 {
		return []HashEntry{}, nil
	}
	bucket = db.resolveBucket(bucket)
	ver, err := db.bucketVersion(bucket)
	if err != nil {
		return nil, err
	}
	if err = validateToken(hkey); err != nil {
		return nil, err
	}
	if start != "" {
		if err = validateToken(start); err != nil {
			return nil, err
		}
	}
	prefix := hashPrefix(bucket, ver, hkey)
	it := db.lv.NewIterator(prefixRange(prefix), nil)
	defer it.Release()

	ok := it.First()
	if start != "" {
		ok = it.Seek(hashFieldKey(bucket, ver, hkey, start))
	}

	keys := make([]string, 0, limit)
	fields := make([]string, 0, limit)
	for ; ok && len(keys) < limit; ok = it.Next() {
		k := it.Key()
		if !bytes.HasPrefix(k, prefix) {
			break
		}
		keys = append(keys, string(k))
		fields = append(fields, string(k[len(prefix):]))
	}

	if len(keys) == 0 {
		return []HashEntry{}, it.Error()
	}

	values, err := db.lv.GetBatch(keys)
	if err != nil {
		return nil, err
	}

	now := time.Now().Unix()
	out := make([]HashEntry, 0, len(keys))
	for i, v := range values {
		if v == nil {
			continue
		}
		expireAt, metaOK := decodeValueMeta(v)
		if !metaOK || isExpired(expireAt, now) {
			continue
		}
		payload, _, decOK, _ := db.decodeValue(v)
		if !decOK {
			continue
		}
		out = append(out, HashEntry{
			Field: fields[i],
			Value: payload,
		})
	}
	return out, nil
}

func (db *DB) HRScan(bucket, hkey, start string, limit int) ([]HashEntry, error) {
	if limit <= 0 {
		return []HashEntry{}, nil
	}
	bucket = db.resolveBucket(bucket)
	ver, err := db.bucketVersion(bucket)
	if err != nil {
		return nil, err
	}
	if err = validateToken(hkey); err != nil {
		return nil, err
	}
	if start != "" {
		if err = validateToken(start); err != nil {
			return nil, err
		}
	}
	prefix := hashPrefix(bucket, ver, hkey)
	it := db.lv.NewIterator(prefixRange(prefix), nil)
	defer it.Release()

	ok := it.Last()
	if start != "" {
		seek := hashFieldKey(bucket, ver, hkey, start)
		if it.Seek(seek) {
			if !bytes.Equal(it.Key(), seek) {
				ok = it.Prev()
			} else {
				ok = true
			}
		} else {
			ok = it.Last()
		}
	}
	now := time.Now().Unix()
	out := make([]HashEntry, 0, limit)
	for ; ok; ok = it.Prev() {
		k := it.Key()
		if !bytes.HasPrefix(k, prefix) {
			break
		}
		expireAt, metaOK := decodeValueMeta(it.Value())
		if !metaOK || isExpired(expireAt, now) {
			continue
		}
		payload, _, decOK, _ := db.decodeValue(it.Value())
		if !decOK {
			continue
		}
		out = append(out, HashEntry{
			Field: string(k[len(prefix):]),
			Value: append([]byte(nil), payload...),
		})
		if len(out) >= limit {
			break
		}
	}
	return out, it.Error()
}

func (db *DB) HDelBucket(bucket, hkey string) error {
	bucket = db.resolveBucket(bucket)
	ver, err := db.bucketVersion(bucket)
	if err != nil {
		return err
	}
	if err = validateToken(hkey); err != nil {
		return err
	}
	prefix := hashPrefix(bucket, ver, hkey)
	it := db.lv.NewIterator(prefixRange(prefix), nil)
	defer it.Release()

	const flushSize = 1024
	batchSize := 0
	var b guixu.Batch
	for ok := it.First(); ok; ok = it.Next() {
		k := it.Key()
		if !bytes.HasPrefix(k, prefix) {
			break
		}
		b.Delete(append([]byte(nil), k...))
		batchSize++
		if batchSize >= flushSize {
			if err := db.lv.Write(&b, nil); err != nil {
				return err
			}
			b.Reset()
			batchSize = 0
		}
	}
	if err := it.Error(); err != nil {
		return err
	}
	b.Delete(hCountKey(bucket, ver, hkey))
	b.Delete(hTTLFlagKey(bucket, ver, hkey))
	return db.lv.Write(&b, nil)
}
