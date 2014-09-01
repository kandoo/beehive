package bh

import "testing"

func maybeSkipRegisteryTest(h *hive, t *testing.T) {
	if len(h.config.RegAddrs) != 0 {
		return
	}

	t.Skip("Registery tests run only when the hive is connected to registery.")
}

func hiveWithAddressForRegisteryTests(addr string, t *testing.T) *hive {
	cfg := DefaultCfg
	cfg.HiveAddr = addr
	return NewHiveWithConfig(cfg).(*hive)
}

func TestRegisteryUnregisterHive(t *testing.T) {
	h := hiveWithAddressForRegisteryTests("127.0.0.1:32771", t)
	joinCh := make(chan interface{})
	maybeSkipRegisteryTest(h, t)

	go h.Start(joinCh)
	h.waitUntilStarted()
	h.Stop()
	<-joinCh

	k, _ := h.registery.hiveRegKeyVal()
	_, err := h.registery.Get(k, false, false)
	if err == nil {
		t.Errorf("Registery entry is not removed.")
	}
}

type testRegisteryWatchHandler struct {
	resCh chan HiveId
}

func (h *testRegisteryWatchHandler) Rcv(msg Msg, ctx RcvContext) {
	switch d := msg.Data().(type) {
	case HiveJoined:
		h.resCh <- d.HiveId
	case HiveLeft:
		h.resCh <- d.HiveId
	}
}

func (h *testRegisteryWatchHandler) Map(msg Msg, ctx MapContext) MapSet {
	return MapSet{{"W", Key(ctx.Hive().Id())}}
}

func TestRegisteryWatchHives(t *testing.T) {
	h1 := hiveWithAddressForRegisteryTests("127.0.0.1:32771", t)
	maybeSkipRegisteryTest(h1, t)

	watchCh := make(chan HiveId)

	app := h1.NewApp("RegisteryWatchHandler")
	hndlr := &testRegisteryWatchHandler{
		resCh: watchCh,
	}
	app.Handle(HiveJoined{}, hndlr)
	app.Handle(HiveLeft{}, hndlr)

	h2Id := "127.0.0.1:32772"
	h2 := hiveWithAddressForRegisteryTests(h2Id, t)
	maybeSkipRegisteryTest(h2, t)

	joinCh1 := make(chan interface{})
	go h1.Start(joinCh1)
	h1.waitUntilStarted()

	joinCh2 := make(chan interface{})
	go h2.Start(joinCh2)
	h2.waitUntilStarted()

	h2.Stop()
	<-joinCh2

	id := <-watchCh
	if id != HiveId(h2Id) {
		t.Errorf("Invalid hive joined: %v", id)
	}
	id = <-watchCh
	if id != HiveId(h2Id) {
		t.Errorf("Invalid hive left: %v", id)
	}

	h1.Stop()
	<-joinCh1
}
