package bh

import (
	"testing"
	"time"
)

type SnoozeTestMsg int

func TestSnooze(t *testing.T) {
	h := NewHive()
	app := h.NewApp("SnoozingTestApp")

	mapF := func(msg Msg, ctx MapContext) MappedCells {
		return MappedCells{{"D", "K"}}
	}

	ch := make(chan bool)
	i := 0
	rcvF := func(msg Msg, ctx RcvContext) error {
		ch <- true
		i++
		if i == 1 {
			ctx.Snooze(time.Millisecond)
		}
		return nil
	}

	app.HandleFunc(SnoozeTestMsg(0), mapF, rcvF)

	h.Emit(SnoozeTestMsg(0))

	go h.Start()

	<-ch
	select {
	case <-ch:
	case <-time.After(10 * time.Millisecond):
		t.Errorf("Did not receive the snoozed message.")
	}

	h.Stop()
}
