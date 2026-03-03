package guixu

import (
	"sync/atomic"
	"time"
)

type scheduler struct {
	stop chan struct{}
	done chan struct{}
}

func startScheduler(interval time.Duration, fn func()) *scheduler {
	if interval <= 0 {
		interval = time.Second
	}
	s := &scheduler{stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(s.done)
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-s.stop:
				return
			case <-t.C:
				fn()
			}
		}
	}()
	return s
}

func (s *scheduler) Stop() {
	if s == nil {
		return
	}
	select {
	case <-s.stop:
		return
	default:
		close(s.stop)
	}
	<-s.done
}

func rateLimitSleep(bytes int64, maxBytesPS int64) {
	if bytes <= 0 || maxBytesPS <= 0 {
		return
	}
	d := time.Second * time.Duration(bytes) / time.Duration(maxBytesPS)
	if d > 0 {
		time.Sleep(d)
	}
}

func inc(u *uint64, v uint64) { atomic.AddUint64(u, v) }
