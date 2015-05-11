// bucket implements a generic, embeddable token bucket algorithm.
package bucket

import (
	"sync/atomic"
	"time"
)

// Represents a token bucket. It is not thread safe.
type Bucket struct {
	tokens     uint64
	max        uint64
	quantum    uint64
	resolution time.Duration
	ticker     *time.Ticker
}

// Rate represents the rate of token generation per second.
type Rate uint64

// Common rates.
const (
	TPS       Rate = 1
	KTPS           = 1000 * TPS
	MTPS           = 1000 * KTPS
	GTPS           = 1000 * MTPS
	Unlimited      = 0
)

const (
	// DefaultResolution is the default ticker resolution for the bucket ticker.
	DefaultResolution time.Duration = 1 * time.Millisecond
)

// NewBucket creates a new bucket with the given rate and the maximum bucket
// size. Resolution is the resolution of the bucket ticker. Use the
// DefaultResolution if unsure.
//
// If rate is Unlimited, it returns a nil pointer and it is safe to call all
// Bucket methods on that nil pointer:
//
//    b := bucket.NewBucket(Unlimited)
//    b.Put(10)
//    b.Get(10)
//
func New(rate Rate, max uint64, resolution time.Duration) (bucket *Bucket) {
	if rate == Unlimited {
		return nil
	}

	bucket = &Bucket{
		tokens: 0,
		max:    max,
	}

	r := uint64(rate)
	d := uint64(time.Second / resolution)

	if r <= d {
		bucket.resolution = time.Second / time.Duration(r)
		bucket.quantum = 1
	} else {
		bucket.resolution = resolution
		// TODO(soheil): We have a rounding error, if rate is not divisible by
		// the resolution.
		bucket.quantum = r / d
	}

	return
}

// Unlimited returns whether the bucket is unlimited.
func (b *Bucket) Unlimited() bool {
	return b == nil
}

// Has returns whether the bucket has at least t tocken.
func (b *Bucket) Has(t uint64) bool {
	if b.Unlimited() {
		return true
	}

	return t <= atomic.LoadUint64(&b.tokens)
}

// Put adds t tokens to the bucket.
func (b *Bucket) Put(t uint64) {
	if b.Unlimited() {
		return
	}

	// We let the tokens go beyond max, and we cap it in Get.
	atomic.AddUint64(&b.tokens, t)
}

// Get allocates t tockens from the buffer if available, or otherwise
// returns false.
func (b *Bucket) Get(t uint64) bool {
	if b.Unlimited() {
		return true
	}

	if b.max < t {
		panic("bucket: request is more than max tokens")
	}

	for {
		oldt := atomic.LoadUint64(&b.tokens)
		if oldt < t {
			return false
		}

		var newt uint64
		if oldt <= b.max {
			newt = oldt - t
		} else {
			newt = b.max - t
		}

		if atomic.CompareAndSwapUint64(&b.tokens, oldt, newt) {
			return true
		}
	}
}

// Tick is called upon each tick received from the bucket ticker.
func (b *Bucket) Tick() {
	b.Put(b.quantum)
}

// Ticker returns the bucket ticker.
//
// For each tick of this ticker, the client should call bucket.Tick(). Also it
// is the client's responsibility to stop the ticker.
//
// If the bucket is unlimited the ticker will never tick (ie, it is stopped).
func (b *Bucket) Ticker() *time.Ticker {
	if b.Unlimited() {
		t := time.NewTicker(DefaultResolution)
		t.Stop()
		return t
	}

	if b.ticker == nil {
		b.ticker = time.NewTicker(b.resolution)
	}
	return b.ticker
}
