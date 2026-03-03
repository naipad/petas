package petas

import "github.com/naipad/petas/guixu"

func (db *DB) Snapshot(dstDir string) error { return db.lv.Snapshot(dstDir) }

func (db *DB) Checkpoint(dstDir string) error { return db.lv.Checkpoint(dstDir) }

func (db *DB) Backup(dstDir string) error { return db.lv.Backup(dstDir) }

func (db *DB) Validate() error { return db.lv.Validate() }

func (db *DB) GetProperty(name string) (string, error) { return db.lv.GetProperty(name) }

func (db *DB) CompactRange(start, limit []byte) error {
	return db.lv.CompactRange(guixu.Range{
		Start: append([]byte(nil), start...),
		Limit: append([]byte(nil), limit...),
	})
}

func (db *DB) SizeOfRange(start, limit []byte) uint64 {
	return db.lv.SizeOf(guixu.Range{
		Start: append([]byte(nil), start...),
		Limit: append([]byte(nil), limit...),
	})
}

func Restore(srcDir, dstDir string) error { return restoreEngine(srcDir, dstDir) }

