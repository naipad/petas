package petas

import (
	"bytes"
	"errors"
	"sort"

	"github.com/naipad/petas/guixu"
)

type engineDB struct {
	inner *guixu.KV
}

func openEngine(path string, opts guixu.Options) (*engineDB, error) {
	kvdb, err := guixu.Open(path, opts)
	if err != nil {
		return nil, err
	}
	return &engineDB{inner: kvdb}, nil
}

func (e *engineDB) Close() error { return e.inner.Close() }

func (e *engineDB) RunValueLogGC(_ float64) error { return e.inner.CompactAll() }

func (e *engineDB) Get(key []byte, _ *guixu.ReadOptions) ([]byte, error) {
	v, err := e.inner.Get(key)
	if errors.Is(err, guixu.ErrNotFound) {
		return nil, guixu.ErrNotFound
	}
	return v, err
}

func (e *engineDB) Put(key, value []byte, _ *guixu.WriteOptions) error {
	return e.inner.Put(key, value)
}

func (e *engineDB) Delete(key []byte, _ *guixu.WriteOptions) error {
	return e.inner.Delete(key)
}

func (e *engineDB) Write(batch *guixu.Batch, wo *guixu.WriteOptions) error {
	if batch == nil || batch.Len() == 0 {
		return nil
	}
	batch.SetDB(e.inner)
	var sync bool
	if wo != nil && wo.Sync {
		sync = true
	}
	return batch.Commit(guixu.WriteOptions{Sync: sync})
}

func (e *engineDB) engineGet(key []byte) ([]byte, error) {
	return e.Get(key, nil)
}

func (e *engineDB) engineWrite(batch *guixu.Batch) error {
	return e.Write(batch, nil)
}

type engineIterator struct {
	eng  *engineDB
	keys [][]byte
	idx  int
	err  error
}

func (it *engineIterator) First() bool {
	if len(it.keys) == 0 {
		it.idx = -1
		return false
	}
	it.idx = 0
	return true
}

func (it *engineIterator) Last() bool {
	if len(it.keys) == 0 {
		it.idx = -1
		return false
	}
	it.idx = len(it.keys) - 1
	return true
}

func (it *engineIterator) Seek(key []byte) bool {
	i := sort.Search(len(it.keys), func(i int) bool {
		return bytes.Compare(it.keys[i], key) >= 0
	})
	if i >= len(it.keys) {
		it.idx = len(it.keys)
		return false
	}
	it.idx = i
	return true
}

func (it *engineIterator) Next() bool {
	if len(it.keys) == 0 {
		it.idx = -1
		return false
	}
	if it.idx < 0 {
		it.idx = 0
		return true
	}
	it.idx++
	return it.idx >= 0 && it.idx < len(it.keys)
}

func (it *engineIterator) Prev() bool {
	if len(it.keys) == 0 {
		it.idx = -1
		return false
	}
	if it.idx < 0 {
		it.idx = len(it.keys) - 1
		return true
	}
	it.idx--
	return it.idx >= 0 && it.idx < len(it.keys)
}

func (it *engineIterator) Valid() bool {
	return it.idx >= 0 && it.idx < len(it.keys)
}

func (it *engineIterator) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return append([]byte(nil), it.keys[it.idx]...)
}

func (it *engineIterator) Value() []byte {
	if !it.Valid() {
		return nil
	}
	v, err := it.eng.Get(it.keys[it.idx], nil)
	if err != nil {
		it.err = err
		return nil
	}
	return v
}

func (it *engineIterator) Release() {}

func (it *engineIterator) Error() error { return it.err }

func (e *engineDB) NewIterator(r *guixu.Range, _ *guixu.ReadOptions) *engineIterator {
	var start, limit []byte
	if r != nil {
		start = r.Start
		limit = r.Limit
	}
	keys := make([][]byte, 0, 1024)
	e.inner.ScanPrefix(nil, limit, 0, func(k []byte) bool {
		if len(start) > 0 && bytes.Compare(k, start) < 0 {
			return true
		}
		keys = append(keys, append([]byte(nil), k...))
		return true
	})
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 })
	return &engineIterator{eng: e, keys: keys, idx: -1}
}

func (e *engineDB) Snapshot(dstDir string) error   { return e.inner.Snapshot(dstDir) }
func (e *engineDB) Checkpoint(dstDir string) error { return e.inner.Checkpoint(dstDir) }
func (e *engineDB) Backup(dstDir string) error     { return e.inner.Backup(dstDir) }
func (e *engineDB) Validate() error                { return e.inner.Validate() }

func (e *engineDB) CompactRange(r guixu.Range) error {
	return e.inner.CompactRange(r.Start, r.Limit)
}

func (e *engineDB) SizeOf(r guixu.Range) uint64 {
	return e.inner.SizeOf(r.Start, r.Limit)
}

func (e *engineDB) GetProperty(name string) (string, error) {
	v, ok := e.inner.GetProperty(name)
	if !ok {
		return "", errors.New("property not found")
	}
	return v, nil
}

func (e *engineDB) GetBatch(keys []string) ([][]byte, error) {
	return e.inner.GetBatch(keys)
}

func restoreEngine(srcDir, dstDir string) error { return guixu.Restore(srcDir, dstDir) }
