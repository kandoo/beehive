package bh

import "testing"

func maybeSkipRegistryTest(h *hive, t *testing.T) {
	if len(h.config.RegAddrs) != 0 {
		return
	}

	t.Skip("Registry tests run only when the hive is connected to registry.")
}

func hiveWithAddressForRegistryTests(addr string, t *testing.T) *hive {
	cfg := DefaultCfg
	cfg.HiveAddr = addr
	return NewHiveWithConfig(cfg).(*hive)
}

func TestRegistryUnregisterHive(t *testing.T) {
	h := hiveWithAddressForRegistryTests("127.0.0.1:32771", t)
	joinCh := make(chan bool)
	maybeSkipRegistryTest(h, t)

	go h.Start(joinCh)
	h.waitUntilStarted()
	h.Stop()
	<-joinCh

	k, _ := h.registry.hiveRegKeyVal()
	_, err := h.registry.Get(k, false, false)
	if err == nil {
		t.Errorf("Registry entry is not removed.")
	}
}

type testRegistryWatchHandler struct {
	resCh chan HiveID
}

func (h *testRegistryWatchHandler) Rcv(msg Msg, ctx RcvContext) error {
	switch d := msg.Data().(type) {
	case HiveJoined:
		h.resCh <- d.HiveID
	case HiveLeft:
		h.resCh <- d.HiveID
	}
	return nil
}

func (h *testRegistryWatchHandler) Map(msg Msg, ctx MapContext) MappedCells {
	return MappedCells{{"W", Key(ctx.Hive().ID())}}
}

func TestRegistryWatchHives(t *testing.T) {
	h1Id := "127.0.0.1:32771"
	h1 := hiveWithAddressForRegistryTests(h1Id, t)
	maybeSkipRegistryTest(h1, t)

	watchCh := make(chan HiveID, 3)

	app := h1.NewApp("RegistryWatchHandler")
	hndlr := &testRegistryWatchHandler{
		resCh: watchCh,
	}
	app.Handle(HiveJoined{}, hndlr)
	app.Handle(HiveLeft{}, hndlr)

	h2Id := "127.0.0.1:32772"
	h2 := hiveWithAddressForRegistryTests(h2Id, t)
	maybeSkipRegistryTest(h2, t)

	joinCh1 := make(chan bool)
	go h1.Start(joinCh1)
	h1.waitUntilStarted()

	id := <-watchCh
	if id != HiveID(h1Id) {
		t.Errorf("Invalid hive joined: %v", id)
	}

	joinCh2 := make(chan bool)
	go h2.Start(joinCh2)
	h2.waitUntilStarted()

	h2.Stop()
	<-joinCh2

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
