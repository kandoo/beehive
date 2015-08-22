// Package randtime provides a ticker with random ticks with an API identical to
// time.Ticker.
package randtime

import (
	"math/rand"
	"time"
)

type Ticker struct {
	C    <-chan time.Time
	stop chan struct{}
	done chan struct{}
}

// Stop stops the ticker.
func (t *Ticker) Stop() {
	select {
	case t.stop <- struct{}{}:
	case <-t.done:
	}
	<-t.done
}

// NewTicker creates a ticker that ticks every d uniformly selected from
// [dur, dur+delta].
func NewTicker(dur time.Duration, delta time.Duration) (ticker *Ticker) {
	ch := make(chan time.Time, 1)
	ticker = &Ticker{
		C:    ch,
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}

	go func() {
		defer close(ticker.done)
		var realTicker <-chan time.Time
		if delta == 0 {
			// Use a ticker to avoid creating a timer per tick.
			realTicker = time.NewTicker(dur).C
		}
		for {
			if delta != 0 {
				wait := dur + time.Duration(rand.Int63n(int64(delta)))
				realTicker = time.After(wait)
			}
			select {
			case tick := <-realTicker:
				ch <- tick
			case <-ticker.stop:
				return
			}
		}
	}()

	return
}
