package beehive

import "time"

// NewTimer returns a detached handler that calls fn per tick.
func NewTimer(tick time.Duration, fn func()) DetachedHandler {
	return timer{
		tick: tick,
		fn:   fn,
		done: make(chan struct{}),
	}
}

type timer struct {
	tick time.Duration
	fn   func()
	done chan struct{}
}

func (t timer) Start(ctx RcvContext) {
	for {
		select {
		case <-time.Tick(t.tick):
			t.fn()
		case <-t.done:
			return
		}
	}
}

func (t timer) Stop(ctx RcvContext) {
	close(t.done)
}

func (t timer) Rcv(msg Msg, ctx RcvContext) error {
	return nil
}
