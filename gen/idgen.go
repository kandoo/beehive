package gen

import (
	"math/rand"
	"sync/atomic"
)

// IDGenerator generates 64-bit IDs.
type IDGenerator interface {
	// GenID generates a new ID. This method is go-routine safe.
	GenID() uint64
}

// SeqIDGen generates sequential IDs.
type SeqIDGen struct {
	id uint64
}

// NewSeqIDGen creates a SeqIDGen that will start its IDs from init+1.
func NewSeqIDGen(init uint64) *SeqIDGen {
	return &SeqIDGen{id: init}
}

func (g *SeqIDGen) GenID() uint64 {
	return atomic.AddUint64(&g.id, 1)
}

// ResetMinTo resets the minimum to be at least id.
func (g *SeqIDGen) StartFrom(id uint64) {
	val := atomic.LoadUint64(&g.id)
	for val < id {
		if atomic.CompareAndSwapUint64(&g.id, val, id) {
			break
		}
		val = atomic.LoadUint64(&g.id)
	}
}

// RandomIDGen generates random IDs.
type RandomIDGen struct{}

func (g *RandomIDGen) GenID() uint64 {
	return uint64(rand.Int63())
}
