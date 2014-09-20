package bh

import (
	"testing"

	"github.com/coreos/go-etcd/etcd"
)

func newRegistryForTest() registry {
	return registry{
		Client:  etcd.NewClient(DefaultCfg.RegAddrs),
		hive:    nil,
		prefix:  regPrefix,
		hiveDir: regHiveDir,
		hiveTTL: regHiveTTL,
		appDir:  regAppDir,
		appTTL:  regAppTTL,
	}
}

func TestRegistryUnregisterHive(t *testing.T) {
	maybeSkipRegistryTest(t)
	h := hiveWithAddressForTest(hiveAddrsForTest(1)[0], t)

	go h.Start()
	h.waitUntilStarted()
	h.Stop()

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
	maybeSkipRegistryTest(t)

	addrs := hiveAddrsForTest(2)
	h1 := hiveWithAddressForTest(addrs[0], t)

	watchCh := make(chan HiveID, 3)

	app := h1.NewApp("RegistryWatchHandler")
	hndlr := &testRegistryWatchHandler{
		resCh: watchCh,
	}
	app.Handle(HiveJoined{}, hndlr)
	app.Handle(HiveLeft{}, hndlr)

	h2 := hiveWithAddressForTest(addrs[1], t)
	maybeSkipRegistryTest(t)

	go h1.Start()
	h1.waitUntilStarted()

	id := <-watchCh
	if id != HiveID(addrs[0]) {
		t.Errorf("Invalid hive joined: %v", id)
	}

	go h2.Start()
	h2.waitUntilStarted()

	h2.Stop()

	id = <-watchCh
	if id != HiveID(addrs[1]) {
		t.Errorf("Invalid hive joined: %v", id)
	}
	id = <-watchCh
	if id != HiveID(addrs[1]) {
		t.Errorf("Invalid hive left: %v", id)
	}

	h1.Stop()
}

func TestCompareAndSet(t *testing.T) {
	if len(DefaultCfg.RegAddrs) == 0 {
		t.Skip("Skipping the registery test: No registery address")
	}

	reg := newRegistryForTest()
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

func TestTryLock(t *testing.T) {
	maybeSkipRegistryTest(t)

	reg := newRegistryForTest()
	if ok := reg.SyncCluster(); !ok {
		t.Error("Cannot sync registry")
	}

	h := HiveID("127.0.0.1:12345")
	a := AppName("TestA1")
	b1 := BeeID{
		HiveID:  h,
		AppName: a,
		ID:      1,
	}

	ch := make(chan bool)
	go func() {
		if err := reg.lockApp(b1); err != nil {
			t.Error("Cannot lock the app")
		}
		ch <- true
		<-ch
		if err := reg.unlockApp(b1); err != nil {
			t.Error("Cannot unlock the app")
		}
		ch <- true
	}()

	<-ch
	b2 := b1
	b2.ID = 2
	if err := reg.tryLockApp(b2); err == nil {
		t.Error("Locked the app while it has been locked")
	}
	ch <- true
	<-ch
	if err := reg.tryLockApp(b2); err != nil {
		t.Error("Locked the app while it has been locked")
	}
	if err := reg.unlockApp(b2); err != nil {
		t.Error("Cannot unlock the app")
	}
}
