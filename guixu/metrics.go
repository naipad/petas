package guixu

import (
	"sync/atomic"
	"time"
)

// Metrics is a stable runtime snapshot for v0.2.
// It provides performance counters and latency percentiles.
type Metrics struct {
	Reads            uint64
	Writes           uint64
	Deletes          uint64
	ReadBytes        uint64
	WriteBytes       uint64
	VLogBytesWritten uint64
	WriteAmplify     float64

	GCRuns      uint64
	GCErrors    uint64
	Compactions uint64
	Stalls      uint64

	AvgReadLatencyNs  float64
	AvgWriteLatencyNs float64
	P99ReadLatencyNs  uint64
	P99WriteLatencyNs uint64
	UptimeSec         float64
}

// metricSet stores atomics to avoid lock on hot paths.
type metricSet struct {
	startedAt int64

	reads      uint64
	writes     uint64
	deletes    uint64
	readBytes  uint64
	writeBytes uint64
	vlogBytes  uint64

	gcRuns      uint64
	gcErrors    uint64
	compactions uint64
	stalls      uint64

	readLatencyNs  uint64
	writeLatencyNs uint64

	readLatencies  [1024]uint64
	writeLatencies [1024]uint64
	readIdx        uint64
	writeIdx       uint64
}

func newMetricSet() metricSet {
	return metricSet{startedAt: time.Now().UnixNano()}
}

func (m *metricSet) snapshot() Metrics {
	out := Metrics{
		Reads:            atomic.LoadUint64(&m.reads),
		Writes:           atomic.LoadUint64(&m.writes),
		Deletes:          atomic.LoadUint64(&m.deletes),
		ReadBytes:        atomic.LoadUint64(&m.readBytes),
		WriteBytes:       atomic.LoadUint64(&m.writeBytes),
		VLogBytesWritten: atomic.LoadUint64(&m.vlogBytes),
		GCRuns:           atomic.LoadUint64(&m.gcRuns),
		GCErrors:         atomic.LoadUint64(&m.gcErrors),
		Compactions:      atomic.LoadUint64(&m.compactions),
		Stalls:           atomic.LoadUint64(&m.stalls),
	}
	if out.WriteBytes > 0 {
		out.WriteAmplify = float64(out.VLogBytesWritten) / float64(out.WriteBytes)
	}
	if out.Reads > 0 {
		out.AvgReadLatencyNs = float64(atomic.LoadUint64(&m.readLatencyNs)) / float64(out.Reads)
	}
	if out.Writes > 0 {
		out.AvgWriteLatencyNs = float64(atomic.LoadUint64(&m.writeLatencyNs)) / float64(out.Writes)
	}
	out.P99ReadLatencyNs = m.p99(m.readLatencies[:])
	out.P99WriteLatencyNs = m.p99(m.writeLatencies[:])
	out.UptimeSec = time.Since(time.Unix(0, m.startedAt)).Seconds()
	return out
}

func (m *metricSet) p99(samples []uint64) uint64 {
	var buf [1024]uint64
	copy(buf[:], samples)
	for i := 0; i < len(buf)-1; i++ {
		for j := i + 1; j < len(buf); j++ {
			if buf[i] > buf[j] {
				buf[i], buf[j] = buf[j], buf[i]
			}
		}
	}
	return buf[1013]
}
