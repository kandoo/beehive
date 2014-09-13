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
	joinCh := make(chan bool)
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
	resCh chan HiveID
}

func (h *testRegisteryWatchHandler) Rcv(msg Msg, ctx RcvContext) error {
	switch d := msg.Data().(type) {
	case HiveJoined:
		h.resCh <- d.HiveID
	case HiveLeft:
		h.resCh <- d.HiveID
	}
	return nil
}

func (h *testRegisteryWatchHandler) Map(msg Msg, ctx MapContext) MappedCells {
	return MappedCells{{"W", Key(ctx.Hive().ID())}}
}

func TestRegisteryWatchHives(t *testing.T) {
	h1Id := "127.0.0.1:32771"
	h1 := hiveWithAddressForRegisteryTests(h1Id, t)
	maybeSkipRegisteryTest(h1, t)

	watchCh := make(chan HiveID, 3)

	app := h1.NewApp("RegisteryWatchHandler")
	hndlr := &testRegisteryWatchHandler{
		resCh: watchCh,
	}
	app.Handle(HiveJoined{}, hndlr)
	app.Handle(HiveLeft{}, hndlr)

	h2Id := "127.0.0.1:32772"
	h2 := hiveWithAddressForRegisteryTests(h2Id, t)
	maybeSkipRegisteryTest(h2, t)

	joinCh1 := make(chan bool)
	go h1.Start(joinCh1)
	h1.waitUntilStarted()

	joinCh2 := make(chan bool)
	go h2.Start(joinCh2)
	h2.waitUntilStarted()

	h2.Stop()
	<-joinCh2

	id := <-watchCh
	if id != HiveID(h1Id) {
		t.Errorf("Invalid hive joined: %v", id)
	}
	id = <-watchCh
	if id != HiveID(h2Id) {
		t.Errorf("Invalid hive joined: %v", id)
	}
	id = <-watchCh
	if id != HiveID(h2Id) {
		t.Errorf("Invalid hive left: %v", id)
	}

	h1.Stop()
	<-joinCh1
}
