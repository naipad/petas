package petas

import (
	"time"

	"github.com/naipad/petas/guixu"
)

func (db *DB) enqueueCleanupDelete(key []byte) {
	if len(key) == 0 || db.cleanupCh == nil {
		return
	}
	cp := append([]byte(nil), key...)
	select {
	case db.cleanupCh <- cp:
	default:
		// Best-effort cleanup; drop when queue is full to protect foreground latency.
	}
}

func (db *DB) startCleanupWorker() {
	if db.cleanupCh == nil || db.cleanupStop == nil || db.cleanupDone == nil {
		return
	}
	go func() {
		defer close(db.cleanupDone)
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		flush := func(batch *guixu.Batch, count *int) {
			if *count == 0 {
				return
			}
			_ = db.lv.Write(batch, nil)
			batch.Reset()
			*count = 0
		}

		var b guixu.Batch
		count := 0
		for {
			select {
			case <-db.cleanupStop:
				flush(&b, &count)
				return
			case k := <-db.cleanupCh:
				b.Delete(k)
				count++
				if count >= 256 {
					flush(&b, &count)
				}
			case <-ticker.C:
				flush(&b, &count)
			}
		}
	}()
}

func (db *DB) stopCleanupWorker() {
	if db.cleanupStop == nil || db.cleanupDone == nil {
		return
	}
	close(db.cleanupStop)
	<-db.cleanupDone
	db.cleanupStop = nil
	db.cleanupDone = nil
	db.cleanupCh = nil
}
