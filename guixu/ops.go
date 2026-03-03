package guixu

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

func (db *KV) Snapshot(dstDir string) error {
	return db.checkpointTo(dstDir)
}

func (db *KV) Checkpoint(dstDir string) error {
	return db.checkpointTo(dstDir)
}

func (db *KV) Backup(dstDir string) error {
	return db.checkpointTo(dstDir)
}

func (db *KV) checkpointTo(dstDir string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		return err
	}
	for _, f := range db.vlog {
		if f != nil {
			if err := f.Sync(); err != nil {
				return err
			}
		}
	}
	if err := db.flushHintLocked(); err != nil {
		return err
	}
	if db.hint != nil {
		if err := db.hint.Sync(); err != nil {
			return err
		}
	}
	if err := writeManifest(db.opts.Dir, db.manifest); err != nil {
		return err
	}
	files, err := os.ReadDir(db.opts.Dir)
	if err != nil {
		return err
	}
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		name := f.Name()
		if !strings.HasPrefix(name, "vlog-") && name != "MANIFEST.v2.json" && name != "EDITLOG.v2.jsonl" && name != "HINT.v2.bin" {
			continue
		}
		if err := copyFile(filepath.Join(db.opts.Dir, name), filepath.Join(dstDir, name)); err != nil {
			return err
		}
	}
	return nil
}

func Restore(srcDir, dstDir string) error {
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		return err
	}
	files, err := os.ReadDir(srcDir)
	if err != nil {
		return err
	}
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		name := f.Name()
		if !strings.HasPrefix(name, "vlog-") && name != "MANIFEST.v2.json" && name != "EDITLOG.v2.jsonl" && name != "HINT.v2.bin" {
			continue
		}
		if err := copyFile(filepath.Join(srcDir, name), filepath.Join(dstDir, name)); err != nil {
			return err
		}
	}
	return nil
}

// RecoverFile rebuilds manifest by replaying edit log at given directory.
func RecoverFile(dir string) error {
	m, ok, err := readManifest(dir)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("guixu: manifest not found")
	}
	m2, err := replayEdits(dir, m)
	if err != nil {
		return err
	}
	return writeManifest(dir, m2)
}

func (db *KV) CompactRange(start, limit []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()
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
	for _, k := range keys {
		val, err := db.getUnlocked([]byte(k))
		if err != nil {
			continue
		}
		ent := db.index[k]
		if err := db.putWithTTLLockedTS([]byte(k), val, ent.ExpireAt, false, time.Now()); err != nil {
			return err
		}
	}
	return nil
}

func (db *KV) GetProperty(name string) (string, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	switch name {
	case "guixu.keys":
		return itoa(len(db.index)), true
	case "guixu.vlog.segments":
		return itoa(len(db.vlog)), true
	case "guixu.active.segment":
		return itoa64(db.activeSeg), true
	case "guixu.vlog.bytes":
		return itoa64(db.Metrics().VLogBytesWritten), true
	case "guixu.readonly":
		if db.manifest.ReadOnly {
			return "1", true
		}
		return "0", true
	default:
		return "", false
	}
}

func (db *KV) SizeOf(start, limit []byte) uint64 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	var total uint64
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
		total += uint64(v.Length)
	}
	return total
}

func (db *KV) Validate() error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	for k, ent := range db.index {
		if ent.Tombstone() {
			continue
		}
		f := db.vlog[ent.SegmentID]
		if f == nil {
			return errors.New("guixu: missing segment for key " + k)
		}
		rec, err := db.readRecordLocked(f, ent.VPtr)
		if err != nil {
			return err
		}
		if string(rec.Key) != k {
			return errors.New("guixu: key mismatch in vlog")
		}
	}
	return nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	_, err = io.Copy(out, in)
	cerr := out.Close()
	if err != nil {
		return err
	}
	return cerr
}

func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	return itoa64(uint64(v))
}

func itoa64(v uint64) string {
	if v == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + (v % 10))
		v /= 10
	}
	return string(b[i:])
}
