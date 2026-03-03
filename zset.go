package petas

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"time"

	"github.com/naipad/petas/guixu"
)

func (db *DB) ZAdd(bucket, zkey string, score float64, member string, ttl time.Duration) error {
	bucket = db.resolveBucket(bucket)
	ver, err := db.bucketVersion(bucket)
	if err != nil {
		return err
	}
	if err = validateToken(zkey); err != nil {
		return err
	}
	if err = validateToken(member); err != nil {
		return err
	}

	mk := zMemberKey(bucket, ver, zkey, member)
	var b guixu.Batch
	old, getErr := db.lv.Get(mk, nil)
	isNew := errors.Is(getErr, guixu.ErrNotFound)
	if getErr != nil && !isNew {
		return getErr
	}
	if !isNew {
		oldScore, ok, stale, rdErr := decodeZScoreValue(old, time.Now().Unix())
		if rdErr == nil && ok && !stale {
			b.Delete(zIndexKey(bucket, ver, zkey, oldScore, member))
		} else if stale {
			b.Delete(zMemberKey(bucket, ver, zkey, member))
			isNew = true
		}
	}
	b.Put(mk, encodeZScoreValue(score, ttl))
	b.Put(zIndexKey(bucket, ver, zkey, score, member), encodeZIndexValue(ttl))
	if ttl > 0 {
		b.Put(zTTLFlagKey(bucket, ver, zkey), []byte{1})
	}

	if isNew {
		cnt := db.loadZCount(bucket, ver, zkey)
		b.Put(zCountKey(bucket, ver, zkey), encodeZCount(cnt+1))
	}
	return db.lv.Write(&b, nil)
}

func (db *DB) ZGet(bucket, zkey, member string) (float64, bool, error) {
	bucket = db.resolveBucket(bucket)
	ver, err := db.bucketVersion(bucket)
	if err != nil {
		return 0, false, err
	}
	if err = validateToken(zkey); err != nil {
		return 0, false, err
	}
	if err = validateToken(member); err != nil {
		return 0, false, err
	}

	now := time.Now().Unix()
	score, ok, stale, err := db.readZMemberScore(bucket, ver, zkey, member, now)
	if err != nil {
		return 0, false, err
	}
	if stale {
		_ = db.cleanupZMember(bucket, ver, zkey, member, score, true)
		return 0, false, nil
	}
	return score, ok, nil
}

func (db *DB) ZDel(bucket, zkey, member string) error {
	bucket = db.resolveBucket(bucket)
	ver, err := db.bucketVersion(bucket)
	if err != nil {
		return err
	}
	if err = validateToken(zkey); err != nil {
		return err
	}
	if err = validateToken(member); err != nil {
		return err
	}

	now := time.Now().Unix()
	s, ok, stale, err := db.readZMemberScore(bucket, ver, zkey, member, now)
	if err != nil {
		return err
	}
	if !ok && !stale {
		return nil
	}
	return db.cleanupZMember(bucket, ver, zkey, member, s, ok || stale)
}

func (db *DB) ZCard(bucket, zkey string) (int, error) {
	bucket = db.resolveBucket(bucket)
	ver, err := db.bucketVersion(bucket)
	if err != nil {
		return 0, err
	}
	if err = validateToken(zkey); err != nil {
		return 0, err
	}
	if !db.hasZTTL(bucket, ver, zkey) {
		if raw, getErr := db.lv.Get(zCountKey(bucket, ver, zkey), nil); getErr == nil {
			return decodeZCount(raw), nil
		}
	}
	prefix := zMemberPrefix(bucket, ver, zkey)
	it := db.lv.NewIterator(prefixRange(prefix), nil)
	defer it.Release()
	now := time.Now().Unix()
	count := 0
	for ok := it.First(); ok; ok = it.Next() {
		if !bytes.HasPrefix(it.Key(), prefix) {
			break
		}
		_, ok, stale, decErr := decodeZScoreValue(it.Value(), now)
		if decErr != nil || !ok || stale {
			continue
		}
		count++
	}
	if itErr := it.Error(); itErr != nil {
		return 0, itErr
	}
	_ = db.lv.Put(zCountKey(bucket, ver, zkey), encodeZCount(count), nil)
	return count, nil
}

func (db *DB) ZRevRange(bucket, zkey string, offset, limit int) ([]string, error) {
	if limit <= 0 {
		return []string{}, nil
	}
	if offset < 0 {
		offset = 0
	}
	bucket = db.resolveBucket(bucket)
	ver, err := db.bucketVersion(bucket)
	if err != nil {
		return nil, err
	}
	if err = validateToken(zkey); err != nil {
		return nil, err
	}

	prefix := zIndexPrefix(bucket, ver, zkey)
	countKey := zCountKey(bucket, ver, zkey)
	hasTTL := db.hasZTTL(bucket, ver, zkey)
	it := db.lv.NewIterator(prefixRange(prefix), nil)
	defer it.Release()
	if !it.Last() {
		return []string{}, nil
	}

	now := time.Now().Unix()
	out := make([]string, 0, limit)
	seen := 0
	var cleanup guixu.Batch
	needsCleanup := false
	for ; it.Valid(); it.Prev() {
		k := it.Key()
		if seen < offset {
			seen++
			continue
		}
		memberRaw, ok := parseZMemberSliceFromIndex(k, prefix)
		if !ok {
			continue
		}
		if hasTTL {
			if isZIndexExpired(it.Value(), now) {
				memberKey := zMemberKey(bucket, ver, zkey, string(memberRaw))
				cleanup.Delete(memberKey)
				cleanup.Delete(cloneBytes(k))
				cleanup.Delete(countKey)
				needsCleanup = true
				continue
			}
		}
		out = append(out, string(memberRaw))
		if len(out) >= limit {
			break
		}
	}
	if needsCleanup {
		cleanup.Delete(countKey)
		if err := db.lv.Write(&cleanup, nil); err != nil {
			return nil, err
		}
	}
	return out, it.Error()
}

func (db *DB) readZMemberScore(bucket string, ver uint64, zkey, member string, now int64) (float64, bool, bool, error) {
	raw, err := db.lv.Get(zMemberKey(bucket, ver, zkey, member), nil)
	if errors.Is(err, guixu.ErrNotFound) {
		return 0, false, false, nil
	}
	if err != nil {
		return 0, false, false, err
	}
	return decodeZScoreValue(raw, now)
}

func (db *DB) cleanupZMember(bucket string, ver uint64, zkey, member string, score float64, hasScore bool) error {
	var b guixu.Batch
	b.Delete(zMemberKey(bucket, ver, zkey, member))
	if hasScore {
		b.Delete(zIndexKey(bucket, ver, zkey, score, member))
	}
	b.Delete(zCountKey(bucket, ver, zkey))
	return db.lv.Write(&b, nil)
}

func encodeZScoreValue(score float64, ttl time.Duration) []byte {
	expireAt := int64(0)
	if ttl > 0 {
		expireAt = time.Now().Add(ttl).Unix()
	}
	out := make([]byte, 18)
	out[0] = valueHeaderVersion
	out[1] = valueCodecNone
	binary.BigEndian.PutUint64(out[2:10], uint64(expireAt))
	binary.BigEndian.PutUint64(out[10:18], math.Float64bits(score))
	return out
}

func decodeZScoreValue(raw []byte, now int64) (float64, bool, bool, error) {
	if len(raw) < 18 || raw[0] != valueHeaderVersion {
		return 0, false, true, nil
	}
	expireAt := int64(binary.BigEndian.Uint64(raw[2:10]))
	score := math.Float64frombits(binary.BigEndian.Uint64(raw[10:18]))
	if isExpired(expireAt, now) {
		return score, false, true, nil
	}
	return score, true, false, nil
}

func encodeZCount(n int) []byte {
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, uint64(n))
	return out
}

func decodeZCount(raw []byte) int {
	if len(raw) == 8 {
		return int(binary.BigEndian.Uint64(raw))
	}
	return 0
}

func (db *DB) loadZCount(bucket string, ver uint64, zkey string) int {
	rawCnt, err := db.lv.Get(zCountKey(bucket, ver, zkey), nil)
	if err == nil {
		return decodeZCount(rawCnt)
	}
	prefix := zMemberPrefix(bucket, ver, zkey)
	it := db.lv.NewIterator(prefixRange(prefix), nil)
	defer it.Release()
	now := time.Now().Unix()
	count := 0
	for ok := it.First(); ok; ok = it.Next() {
		k := it.Key()
		if !bytes.HasPrefix(k, prefix) {
			break
		}
		_, okV, stale, decErr := decodeZScoreValue(it.Value(), now)
		if decErr != nil || !okV || stale {
			continue
		}
		count++
	}
	if err := it.Error(); err != nil {
		return 0
	}
	_ = db.lv.Put(zCountKey(bucket, ver, zkey), encodeZCount(count), nil)
	return count
}

func cloneBytes(src []byte) []byte {
	out := make([]byte, len(src))
	copy(out, src)
	return out
}

func encodeZIndexValue(ttl time.Duration) []byte {
	if ttl <= 0 {
		return nil
	}
	expireAt := time.Now().Add(ttl).Unix()
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, uint64(expireAt))
	return out
}

func isZIndexExpired(raw []byte, now int64) bool {
	if len(raw) != 8 {
		return false
	}
	expireAt := int64(binary.BigEndian.Uint64(raw))
	return isExpired(expireAt, now)
}

func (db *DB) hasZTTL(bucket string, ver uint64, zkey string) bool {
	_, err := db.lv.Get(zTTLFlagKey(bucket, ver, zkey), nil)
	return err == nil
}

type ZEntry struct {
	Member string
	Score  float64
}

func (db *DB) ZHas(bucket, zkey, member string) (bool, error) {
	_, ok, err := db.ZGet(bucket, zkey, member)
	return ok, err
}

func (db *DB) ZMSet(bucket, zkey string, members map[string]float64, ttl time.Duration) error {
	if len(members) == 0 {
		return nil
	}
	bucket = db.resolveBucket(bucket)
	ver, err := db.bucketVersion(bucket)
	if err != nil {
		return err
	}
	if err = validateToken(zkey); err != nil {
		return err
	}
	var b guixu.Batch
	now := time.Now().Unix()
	isTTL := ttl > 0
	count := db.loadZCount(bucket, ver, zkey)
	added := 0

	for member, score := range members {
		if err = validateToken(member); err != nil {
			return err
		}
		mk := zMemberKey(bucket, ver, zkey, member)
		old, getErr := db.lv.Get(mk, nil)
		isNew := errors.Is(getErr, guixu.ErrNotFound)
		if getErr != nil && !isNew {
			return getErr
		}
		if getErr == nil {
			oldScore, ok, stale, rdErr := decodeZScoreValue(old, now)
			if rdErr == nil && ok && !stale {
				b.Delete(zIndexKey(bucket, ver, zkey, oldScore, member))
				isNew = false
			} else {
				b.Delete(mk)
				isNew = true
			}
		}
		b.Put(mk, encodeZScoreValue(score, ttl))
		b.Put(zIndexKey(bucket, ver, zkey, score, member), encodeZIndexValue(ttl))
		if isNew {
			added++
		}
	}
	if added > 0 {
		b.Put(zCountKey(bucket, ver, zkey), encodeZCount(count+added))
	}
	if isTTL {
		b.Put(zTTLFlagKey(bucket, ver, zkey), []byte{1})
	}
	return db.lv.Write(&b, nil)
}

func (db *DB) ZMGet(bucket, zkey string, members ...string) (map[string]float64, error) {
	out := make(map[string]float64, len(members))
	err := db.ZMGetEach(bucket, zkey, func(member string, score float64) bool {
		out[member] = score
		return true
	}, members...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (db *DB) ZMGetEach(bucket, zkey string, fn func(member string, score float64) bool, members ...string) error {
	bucket = db.resolveBucket(bucket)
	ver, err := db.bucketVersion(bucket)
	if err != nil {
		return err
	}
	if err = validateToken(zkey); err != nil {
		return err
	}
	prefix := zMemberPrefix(bucket, ver, zkey)
	keyBuf := make([]byte, 0, len(prefix)+64)
	hasTTL := db.hasZTTL(bucket, ver, zkey)
	now := time.Now().Unix()
	for _, member := range members {
		if err = validateToken(member); err != nil {
			return err
		}
		keyBuf = append(keyBuf[:0], prefix...)
		keyBuf = append(keyBuf, member...)
		raw, getErr := db.lv.Get(keyBuf, nil)
		if getErr != nil {
			if getErr == guixu.ErrNotFound {
				continue
			}
			return getErr
		}
		if !hasTTL {
			if score, ok := decodeZScoreValueNoTTL(raw); ok {
				if !fn(member, score) {
					return nil
				}
				continue
			}
		}
		score, ok, stale, derr := decodeZScoreValue(raw, now)
		if derr != nil {
			return derr
		}
		if stale {
			_ = db.cleanupZMember(bucket, ver, zkey, member, score, true)
			continue
		}
		if ok && !fn(member, score) {
			return nil
		}
	}
	return nil
}

func decodeZScoreValueNoTTL(raw []byte) (float64, bool) {
	if len(raw) < 18 || raw[0] != valueHeaderVersion {
		return 0, false
	}
	if binary.BigEndian.Uint64(raw[2:10]) != 0 {
		return 0, false
	}
	return math.Float64frombits(binary.BigEndian.Uint64(raw[10:18])), true
}

func (db *DB) ZMDel(bucket, zkey string, members ...string) error {
	if len(members) == 0 {
		return nil
	}
	bucket = db.resolveBucket(bucket)
	ver, err := db.bucketVersion(bucket)
	if err != nil {
		return err
	}
	if err = validateToken(zkey); err != nil {
		return err
	}
	now := time.Now().Unix()
	var b guixu.Batch
	removedAny := false
	for _, member := range members {
		if err = validateToken(member); err != nil {
			return err
		}
		mk := zMemberKey(bucket, ver, zkey, member)
		raw, getErr := db.lv.Get(mk, nil)
		if errors.Is(getErr, guixu.ErrNotFound) {
			continue
		}
		if getErr != nil {
			return getErr
		}
		score, ok, stale, derr := decodeZScoreValue(raw, now)
		if derr != nil {
			return derr
		}
		if !ok && !stale {
			continue
		}
		b.Delete(mk)
		b.Delete(zIndexKey(bucket, ver, zkey, score, member))
		removedAny = true
	}
	if removedAny {
		b.Delete(zCountKey(bucket, ver, zkey))
	}
	if err := db.lv.Write(&b, nil); err != nil {
		return err
	}
	return nil
}

func (db *DB) ZIncr(bucket, zkey, member string, delta float64, ttl time.Duration) (float64, error) {
	bucket = db.resolveBucket(bucket)
	lockKey := "zincr:" + bucket + ":" + zkey + ":" + member
	db.lockKey(lockKey)
	defer db.unlockKey(lockKey)

	ver, err := db.bucketVersion(bucket)
	if err != nil {
		return 0, err
	}
	if err = validateToken(zkey); err != nil {
		return 0, err
	}
	if err = validateToken(member); err != nil {
		return 0, err
	}

	mk := zMemberKey(bucket, ver, zkey, member)
	now := time.Now().Unix()
	cur := 0.0
	isNew := false
	var b guixu.Batch
	old, getErr := db.lv.Get(mk, nil)
	if errors.Is(getErr, guixu.ErrNotFound) {
		isNew = true
	} else if getErr != nil {
		return 0, getErr
	} else {
		oldScore, ok, stale, derr := decodeZScoreValue(old, now)
		if derr != nil {
			return 0, derr
		}
		if ok && !stale {
			cur = oldScore
			b.Delete(zIndexKey(bucket, ver, zkey, oldScore, member))
		} else {
			isNew = true
			b.Delete(mk)
			b.Delete(zIndexKey(bucket, ver, zkey, oldScore, member))
		}
	}
	next := cur + delta
	b.Put(mk, encodeZScoreValue(next, ttl))
	b.Put(zIndexKey(bucket, ver, zkey, next, member), encodeZIndexValue(ttl))
	if isNew {
		cnt := db.loadZCount(bucket, ver, zkey)
		b.Put(zCountKey(bucket, ver, zkey), encodeZCount(cnt+1))
	}
	if ttl > 0 {
		b.Put(zTTLFlagKey(bucket, ver, zkey), []byte{1})
	}
	if err := db.lv.Write(&b, nil); err != nil {
		return 0, err
	}
	return next, nil
}

func (db *DB) ZRank(bucket, zkey, member string) (int, bool, error) {
	return db.zRank(bucket, zkey, member, false)
}

func (db *DB) ZRevRank(bucket, zkey, member string) (int, bool, error) {
	return db.zRank(bucket, zkey, member, true)
}

func (db *DB) zRank(bucket, zkey, member string, reverse bool) (int, bool, error) {
	if err := validateToken(member); err != nil {
		return 0, false, err
	}
	rank := 0
	found := false
	err := db.walkZIndex(bucket, zkey, reverse, func(cur ZEntry) bool {
		if cur.Member == member {
			found = true
			return false
		}
		rank++
		return true
	})
	return rank, found, err
}

func (db *DB) ZRange(bucket, zkey string, offset, limit int) ([]string, error) {
	return db.zRange(bucket, zkey, offset, limit, false)
}

func (db *DB) zRange(bucket, zkey string, offset, limit int, reverse bool) ([]string, error) {
	if limit <= 0 {
		return []string{}, nil
	}
	if offset < 0 {
		offset = 0
	}
	out := make([]string, 0, limit)
	seen := 0
	err := db.walkZIndex(bucket, zkey, reverse, func(cur ZEntry) bool {
		if seen < offset {
			seen++
			return true
		}
		out = append(out, cur.Member)
		return len(out) < limit
	})
	return out, err
}

func (db *DB) ZRangeByScore(bucket, zkey string, min, max float64, offset, limit int) ([]string, error) {
	if limit <= 0 {
		return []string{}, nil
	}
	if offset < 0 {
		offset = 0
	}
	out := make([]string, 0, limit)
	seen := 0
	err := db.walkZIndex(bucket, zkey, false, func(cur ZEntry) bool {
		if cur.Score < min || cur.Score > max {
			return true
		}
		if seen < offset {
			seen++
			return true
		}
		out = append(out, cur.Member)
		return len(out) < limit
	})
	return out, err
}

func (db *DB) ZCount(bucket, zkey string, min, max float64) (int, error) {
	count := 0
	err := db.walkZIndex(bucket, zkey, false, func(cur ZEntry) bool {
		if cur.Score >= min && cur.Score <= max {
			count++
		}
		return true
	})
	return count, err
}

func (db *DB) ZRemRangeByRank(bucket, zkey string, start, stop int) (int, error) {
	card, err := db.ZCard(bucket, zkey)
	if err != nil {
		return 0, err
	}
	start, stop, ok := normalizeIndexRange(start, stop, card)
	if !ok {
		return 0, nil
	}
	toDel := make([]ZEntry, 0, stop-start+1)
	idx := 0
	err = db.walkZIndex(bucket, zkey, false, func(cur ZEntry) bool {
		if idx >= start && idx <= stop {
			toDel = append(toDel, cur)
		}
		idx++
		return idx <= stop
	})
	if err != nil {
		return 0, err
	}
	if err := db.deleteZEntries(bucket, zkey, toDel); err != nil {
		return 0, err
	}
	return len(toDel), nil
}

func (db *DB) ZRemRangeByScore(bucket, zkey string, min, max float64) (int, error) {
	toDel := make([]ZEntry, 0, 64)
	err := db.walkZIndex(bucket, zkey, false, func(cur ZEntry) bool {
		if cur.Score >= min && cur.Score <= max {
			toDel = append(toDel, cur)
		}
		return true
	})
	if err != nil {
		return 0, err
	}
	if err := db.deleteZEntries(bucket, zkey, toDel); err != nil {
		return 0, err
	}
	return len(toDel), nil
}

func (db *DB) ZDelBucket(bucket, zkey string) error {
	bucket = db.resolveBucket(bucket)
	ver, err := db.bucketVersion(bucket)
	if err != nil {
		return err
	}
	if err = validateToken(zkey); err != nil {
		return err
	}
	memberPrefix := zMemberPrefix(bucket, ver, zkey)
	indexPrefix := zIndexPrefix(bucket, ver, zkey)

	if err := db.deleteByPrefix(memberPrefix); err != nil {
		return err
	}
	if err := db.deleteByPrefix(indexPrefix); err != nil {
		return err
	}
	var b guixu.Batch
	b.Delete(zCountKey(bucket, ver, zkey))
	b.Delete(zTTLFlagKey(bucket, ver, zkey))
	return db.lv.Write(&b, nil)
}

func (db *DB) walkZIndex(bucket, zkey string, reverse bool, fn func(entry ZEntry) bool) error {
	bucket = db.resolveBucket(bucket)
	ver, err := db.bucketVersion(bucket)
	if err != nil {
		return err
	}
	if err = validateToken(zkey); err != nil {
		return err
	}
	prefix := zIndexPrefix(bucket, ver, zkey)
	countKey := zCountKey(bucket, ver, zkey)
	hasTTL := db.hasZTTL(bucket, ver, zkey)
	it := db.lv.NewIterator(prefixRange(prefix), nil)
	defer it.Release()

	next := it.First
	step := it.Next
	if reverse {
		next = it.Last
		step = it.Prev
	}
	if !next() {
		return nil
	}
	now := time.Now().Unix()
	var cleanup guixu.Batch
	needsCleanup := false
	for ; it.Valid(); step() {
		k := it.Key()
		if !bytes.HasPrefix(k, prefix) {
			break
		}
		memberRaw, ok := parseZMemberSliceFromIndex(k, prefix)
		if !ok {
			continue
		}
		score, ok := parseZScoreFromIndex(k, prefix)
		if !ok {
			continue
		}
		if hasTTL && isZIndexExpired(it.Value(), now) {
			cleanup.Delete(zMemberKey(bucket, ver, zkey, string(memberRaw)))
			cleanup.Delete(append([]byte(nil), k...))
			needsCleanup = true
			continue
		}
		if !fn(ZEntry{Member: string(memberRaw), Score: score}) {
			break
		}
	}
	if needsCleanup {
		cleanup.Delete(countKey)
		if err := db.lv.Write(&cleanup, nil); err != nil {
			return err
		}
	}
	return it.Error()
}

func (db *DB) deleteZEntries(bucket, zkey string, entries []ZEntry) error {
	if len(entries) == 0 {
		return nil
	}
	bucket = db.resolveBucket(bucket)
	ver, err := db.bucketVersion(bucket)
	if err != nil {
		return err
	}
	if err = validateToken(zkey); err != nil {
		return err
	}
	var b guixu.Batch
	for _, e := range entries {
		b.Delete(zMemberKey(bucket, ver, zkey, e.Member))
		b.Delete(zIndexKey(bucket, ver, zkey, e.Score, e.Member))
	}
	b.Delete(zCountKey(bucket, ver, zkey))
	return db.lv.Write(&b, nil)
}

func (db *DB) deleteByPrefix(prefix []byte) error {
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
	if batchSize > 0 {
		return db.lv.Write(&b, nil)
	}
	return nil
}

func normalizeIndexRange(start, stop, n int) (int, int, bool) {
	if n <= 0 {
		return 0, 0, false
	}
	if start < 0 {
		start = n + start
	}
	if stop < 0 {
		stop = n + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= n {
		stop = n - 1
	}
	if start > stop || start >= n {
		return 0, 0, false
	}
	return start, stop, true
}
