package beehive

import "time"

type Timer struct {
	Tick time.Duration
	Func func()
	done chan struct{}
}

func (t Timer) Start(ctx RcvContext) {
	for {
		select {
		case <-time.Tick(t.Tick):
			t.Func()
		case <-t.done:
			return
		}
	}
}

func (t Timer) Stop(ctx RcvContext) {
	close(t.done)
}

func (t Timer) Rcv(msg Msg, ctx RcvContext) error {
	return nil
}
