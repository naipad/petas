package petas

import "github.com/naipad/petas/guixu"

func prefixRange(prefix []byte) *guixu.Range {
	return &guixu.Range{
		Start: append([]byte(nil), prefix...),
		Limit: prefixUpperBound(prefix),
	}
}

func prefixUpperBound(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}
	out := append([]byte(nil), prefix...)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] < 0xFF {
			out[i]++
			return out[:i+1]
		}
	}
	return nil
}
