package beehive

import (
	"strconv"
	"testing"

	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/golang/glog"
)

type testNonLocalPlacementMethod struct{}

func (m testNonLocalPlacementMethod) Place(cells MappedCells, thisHive Hive,
	liveHives []HiveInfo) HiveInfo {

	for _, h := range liveHives {
		if h.ID != thisHive.ID() {
			return h
		}
	}
	glog.Fatal("cannot find a non-local hive")
	return HiveInfo{}
}

type testPlacementRes struct {
	hive uint64
	msg  int
}

func registerPlacementApp(h Hive, ch chan testPlacementRes) App {
	a := h.NewApp("placementapp", NonTransactional(),
		WithPlacement(testNonLocalPlacementMethod{}))
	mf := func(msg Msg, ctx MapContext) MappedCells {
		return MappedCells{{"D", strconv.Itoa(msg.Data().(int))}}
	}
	rf := func(msg Msg, ctx RcvContext) error {
		ctx.Hive().ID()
		ch <- testPlacementRes{
			hive: ctx.Hive().ID(),
			msg:  msg.Data().(int),
		}
		return nil
	}
	a.HandleFunc(int(0), mf, rf)
	return a
}

func TestPlacement(t *testing.T) {
	// TODO(soheil): refactor all these into a single test utility method.
	ch := make(chan testPlacementRes)
	var hives []Hive

	nh := 2
	for i := 0; i < nh; i++ {
		var h Hive
		if i == 0 {
			h = newHiveForTest()
		} else {
			h = newHiveForTest(PeerAddrs(hives[0].(*hive).config.Addr))
		}
		hives = append(hives, h)
		registerPlacementApp(hives[i], ch)
		go hives[i].Start()
		waitTilStareted(hives[i])
	}

	for i := 0; i < nh; i++ {
		go hives[i].Emit(i)
	}

	for i := 0; i < nh; i++ {
		res := <-ch
		if res.hive == hives[res.msg].ID() {
			t.Errorf("Got the message from the same hive")
		}
	}
}
