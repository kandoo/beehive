package bh

import (
	"testing"

	"github.com/coreos/go-etcd/etcd"
)

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

func TestCompareAndSet(t *testing.T) {
	if len(DefaultCfg.RegAddrs) == 0 {
		t.Skip("Skipping the registery test: No registery address")
	}

	reg := registry{
		Client:  etcd.NewClient(DefaultCfg.RegAddrs),
		hive:    nil,
		prefix:  regPrefix,
		hiveDir: regHiveDir,
		hiveTTL: regHiveTTL,
		appDir:  regAppDir,
		appTTL:  regAppTTL,
	}

	if ok := reg.SyncCluster(); !ok {
		t.Error("Cannot sync registry")
	}

	h := HiveID("127.0.0.1:12345")
	a := AppName("TestA1")
	col1 := BeeColony{
		Master: BeeID{
			HiveID:  h,
			AppName: a,
			ID:      1,
		},
		Slaves:     nil,
		Generation: 1,
	}

	col2 := col1
	col2.Generation++

	cells := MappedCells{{"D", "K"}}

	reg.syncCall(col1.Master, func() {
		if b := reg.set(col1, cells); b != col1.Master {
			t.Errorf("Cannot set in registery %#v != %#v", b, col1.Master)
		}

		if _, err := reg.compareAndSet(col1, col2, cells); err != nil {
			t.Errorf("Error in compare and swap: %v", err)
		}
	})
}
