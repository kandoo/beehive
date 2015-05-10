package beehive

import (
	"testing"

	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/golang/glog"
)

type nonLocalPlacementMethod struct{}

func (m nonLocalPlacementMethod) Place(cells MappedCells, thisHive Hive,
	liveHives []HiveInfo) HiveInfo {

	for _, h := range liveHives {
		if h.ID != thisHive.ID() {
			return h
		}
	}
	glog.Fatal("cannot find a non-local hive")
	return HiveInfo{}
}

func registerPlacementApp(h Hive, ch chan uint64) App {
	a := h.NewApp("placementapp", NonTransactional(),
		WithPlacement(nonLocalPlacementMethod{}))
	mf := func(msg Msg, ctx MapContext) MappedCells {
		return MappedCells{{"D", "Centralized"}}
	}
	rf := func(msg Msg, ctx RcvContext) error {
		ch <- ctx.Hive().ID()
		return nil
	}
	a.HandleFunc(int(0), mf, rf)
	return a
}

func TestPlacement(t *testing.T) {
	// TODO(soheil): refactor all these into a single test utility method.
	ch := make(chan uint64)

	cfg1 := DefaultCfg
	cfg1.StatePath = "/tmp/bhtest1"
	cfg1.Addr = newHiveAddrForTest()
	removeState(cfg1)
	h1 := NewHiveWithConfig(cfg1)
	registerPlacementApp(h1, ch)
	go h1.Start()
	waitTilStareted(h1)

	cfg2 := DefaultCfg
	cfg2.StatePath = "/tmp/bhtest2"
	cfg2.Addr = newHiveAddrForTest()
	cfg2.PeerAddrs = []string{cfg1.Addr}
	removeState(cfg2)
	h2 := NewHiveWithConfig(cfg2)
	registerPlacementApp(h2, ch)
	go h2.Start()
	waitTilStareted(h2)

	h1.Emit(int(0))
	id := <-ch
	if id == h1.ID() {
		t.Errorf("received on an incorrect hive: hiveid=%d want=%d", id, h2.ID())
	}
}
