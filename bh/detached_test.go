package bh

import "testing"

type testDetachedHandler struct {
	ch chan bool
}

func (d *testDetachedHandler) Start(ctx RcvContext) {
	d.ch <- true
}

func (d *testDetachedHandler) Stop(ctx RcvContext) {
}

func (d *testDetachedHandler) Rcv(msg Msg, ctx RcvContext) error {
	return nil
}

func TestDetached(t *testing.T) {
	h := NewHive()
	app := h.NewApp("TestDetached")

	nDetached := 10

	msgCh := make(chan bool)
	for i := 0; i < nDetached; i++ {
		app.Detached(&testDetachedHandler{msgCh})
	}

	joinCh := make(chan bool)
	go h.Start(joinCh)

	for i := 0; i < nDetached; i++ {
		<-msgCh
	}

	h.Stop()
}
