package petas

import "sync"

type refMutex struct {
	mu   sync.Mutex
	refs int
}

func (db *DB) lockKey(key string) {
	db.keyLocksMu.Lock()
	rm := db.keyLocks[key]
	if rm == nil {
		rm = &refMutex{}
		db.keyLocks[key] = rm
	}
	rm.refs++
	db.keyLocksMu.Unlock()
	rm.mu.Lock()
}

func (db *DB) unlockKey(key string) {
	db.keyLocksMu.Lock()
	rm := db.keyLocks[key]
	if rm != nil {
		rm.refs--
		if rm.refs <= 0 {
			delete(db.keyLocks, key)
		}
	}
	db.keyLocksMu.Unlock()
	if rm != nil {
		rm.mu.Unlock()
	}
}
