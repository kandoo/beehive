package bh

import (
	"testing"
	"time"
)

type testDetachedHandler struct {
	ch   chan bool
	fork bool
}

type testDetachedMsg int

func (d *testDetachedHandler) Start(ctx RcvContext) {
	if !d.fork {
		d.ch <- true
		return
	}

	b := ctx.StartDetached(&testDetachedHandler{
		ch:   d.ch,
		fork: false,
	})

	msg := testDetachedMsg(0)
	ctx.SendToBee(msg, b)
}

func (d *testDetachedHandler) Stop(ctx RcvContext) {
}

func (d *testDetachedHandler) Rcv(msg Msg, ctx RcvContext) error {
	d.ch <- true
	return nil
}

func TestDetached(t *testing.T) {
	h := NewHive()
	app := h.NewApp("TestDetached")

	nDetached := 10

	msgCh := make(chan bool)
	for i := 0; i < nDetached; i++ {
		app.Detached(&testDetachedHandler{
			ch:   msgCh,
			fork: true,
		})
	}

	go h.Start()

	for i := 0; i < nDetached*2; i++ {
		select {
		case <-msgCh:
		case <-time.After(2 * time.Second):
			t.Errorf("Did not receive any message on the channel.")
		}
	}

	h.Stop()
}
