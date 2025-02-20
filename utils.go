package redis4rag

import (
	"encoding/binary"
	"math"
	"unsafe"
)

func vector2string(v []float64) string {
	b := make([]byte, len(v)*8)
	for i, e := range v {
		i := i * 8
		binary.LittleEndian.PutUint64(b[i:i+8], math.Float64bits(e))
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}
