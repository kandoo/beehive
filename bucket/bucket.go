// bucket implements a generic, embeddable token bucket algorithm.
package bucket

import (
	"sync"
	"time"
)

// Represents a token bucket. It is not thread safe.
type Bucket struct {
	sync.Mutex
	tokens     uint64
	max        uint64
	quantum    uint64
	resolution time.Duration
	timestamp  time.Time
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

// NewBucket creates a new bucket with the given rate and the maximum bucket
// size.
//
// If rate is Unlimited, it returns a nil pointer and it is safe to call all
// Bucket methods on that nil pointer:
//
//    b := bucket.NewBucket(Unlimited)
//    b.Has(10)
//    b.Get(10)
//
func New(rate Rate, max uint64) (bucket *Bucket) {
	if rate == Unlimited {
		return nil
	}

	if max == 0 {
		max = ^uint64(0)
	}

	bucket = &Bucket{
		tokens:     0,
		max:        max,
		quantum:    uint64(rate),
		resolution: time.Second,
		timestamp:  time.Now(),
	}
	bucket.minimizeResolution()
	return bucket
}

func gcd(a, b uint64) uint64 {
	if a < b {
		return gcd(b, a)
	}

	if b == 0 {
		return a
	}

	if b == 1 {
		return 1
	}

	return gcd(b, a%b)
}

func (b *Bucket) minimizeResolution() {
	d := gcd(b.quantum, uint64(b.resolution))
	b.quantum /= d
	b.resolution /= time.Duration(d)
}

func (b *Bucket) has(tokens uint64) bool {
	return tokens <= b.tokens
}

func (b *Bucket) fill() {
	n := time.Now()
	d := n.Sub(b.timestamp)
	if d < b.resolution {
		return
	}

	t := b.tokens + uint64(d/b.resolution)*b.quantum
	if t < b.max {
		b.tokens = t
	} else {
		b.tokens = b.max
	}
	b.timestamp = n
}

// Unlimited returns whether the bucket is unlimited.
func (b *Bucket) Unlimited() bool {
	return b == nil
}

// Max returns the maximum number of tokens that this bucket can store.
func (b *Bucket) Max() uint64 {
	if b.Unlimited() {
		return ^uint64(0)
	}
	return b.max
}

// Reset reset the bucket to an empty bucket and starts adding tokens from now.
func (b *Bucket) Reset() {
	if b.Unlimited() {
		return
	}

	b.Lock()
	b.timestamp = time.Now()
	b.tokens = 0
	b.Unlock()
}

// Has returns whether the bucket has at least t tocken.
func (b *Bucket) Has(tokens uint64) (has bool) {
	if b.Unlimited() {
		return true
	}

	b.Lock()
	b.fill()
	has = b.has(tokens)
	b.Unlock()
	return
}

// When returns the minimum time to wait before enough tokens are available.
func (b *Bucket) When(tokens uint64) (dur time.Duration) {
	if b.Unlimited() || b.max < tokens {
		return 0
	}

	if b.Has(tokens) {
		return 0
	}

	b.Lock()
	if b.has(tokens) {
		b.Unlock()
		return 0
	}

	t := tokens - b.tokens
	dur = time.Duration(t / b.quantum)
	if t%b.quantum != 0 {
		dur++
	}
	dur *= b.resolution
	b.Unlock()
	return dur
}

// Get allocates t tockens from the buffer if available, or otherwise
// returns false.
func (b *Bucket) Get(tokens uint64) (ok bool) {
	if b.Unlimited() {
		return true
	}

	if b.max < tokens {
		panic("bucket: request is more than max tokens")
	}

	b.Lock()
	b.fill()
	if tokens <= b.tokens {
		ok = true
		b.tokens -= tokens
	}
	b.Unlock()
	return
}
