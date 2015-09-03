package beehive

import (
	"fmt"
	"net/http"
	"testing"
	"time"
)

type AppTestMsg int

func TestPersistentApp(t *testing.T) {
	h := newHiveForTest()
	app := h.NewApp("persistent", Persistent(3))
	mf := func(msg Msg, ctx MapContext) MappedCells {
		return ctx.LocalMappedCells()
	}
	ch := make(chan struct{})
	rf := func(msg Msg, ctx RcvContext) error {
		ctx.Dict("Test").Put("K", []byte{})
		ch <- struct{}{}
		return nil
	}
	app.HandleFunc(AppTestMsg(0), mf, rf)

	go h.Start()
	waitTilStareted(h)

	h.Emit(AppTestMsg(0))
	<-ch
	h.Emit(AppTestMsg(0))
	<-ch

	h.Stop()
}

type hiveAndBeeID struct {
	Hive uint64
	Bee  uint64
}

func registerPersistentApp(h Hive, ch chan hiveAndBeeID) App {
	app := h.NewApp("persistent", Persistent(3))
	mf := func(msg Msg, ctx MapContext) MappedCells {
		return MappedCells{{"D", "0"}}
	}
	rf := func(msg Msg, ctx RcvContext) error {
		ctx.Dict("Test").Put("K", []byte{})
		ch <- hiveAndBeeID{
			Hive: h.ID(),
			Bee:  ctx.ID(),
		}
		return nil
	}
	app.HandleFunc(AppTestMsg(0), mf, rf)
	return app
}

func TestReplicatedApp(t *testing.T) {
	ch := make(chan hiveAndBeeID)

	h1 := newHiveForTest()
	registerPersistentApp(h1, ch)
	go h1.Start()
	waitTilStareted(h1)

	cfg1 := h1.Config()

	h2 := newHiveForTest(PeerAddrs(cfg1.Addr))
	registerPersistentApp(h2, ch)
	go h2.Start()
	waitTilStareted(h2)

	h3 := newHiveForTest(PeerAddrs(cfg1.Addr))
	registerPersistentApp(h3, ch)
	go h3.Start()
	waitTilStareted(h3)

	h1.Emit(AppTestMsg(0))
	<-ch
	h1.Emit(AppTestMsg(0))
	<-ch

	time.Sleep(cfg1.RaftElectTimeout())
	h1.Stop()
	h2.Stop()
	h3.Stop()
}

func TestReplicatedAppFailure(t *testing.T) {
	ch := make(chan hiveAndBeeID)

	h1 := newHiveForTest()
	registerPersistentApp(h1, ch)
	go h1.Start()
	waitTilStareted(h1)

	cfg1 := h1.Config()

	h2 := newHiveForTest(PeerAddrs(cfg1.Addr))
	registerPersistentApp(h2, ch)
	go h2.Start()
	waitTilStareted(h2)

	h3 := newHiveForTest(PeerAddrs(cfg1.Addr))
	registerPersistentApp(h3, ch)
	go h3.Start()
	waitTilStareted(h3)

	h1.Emit(AppTestMsg(0))
	<-ch
	h1.Emit(AppTestMsg(0))
	<-ch

	elect := cfg1.RaftElectTimeout()
	time.Sleep(3 * elect)
	h1.Stop()
	time.Sleep(3 * elect)

	for {
		if _, err := h2.(*hive).processCmd(cmdSync{}); err == nil {
			break
		}
		t.Logf("cannot sync %v, retrying", h2)
		time.Sleep(elect)
	}
	for {
		if _, err := h3.(*hive).processCmd(cmdSync{}); err == nil {
			break
		}
		t.Logf("cannot sync %v, retrying", h3)
		time.Sleep(elect)
	}

	time.Sleep(elect)
	h2.Emit(AppTestMsg(0))
	id1 := <-ch
	h3.Emit(AppTestMsg(0))
	id2 := <-ch
	if id1 != id2 {
		t.Errorf("different bees want=%v got=%v", id1, id2)
	}

	time.Sleep(elect)
	h2.Stop()
	h3.Stop()
}

func TestReplicatedAppHandoff(t *testing.T) {
	ch := make(chan hiveAndBeeID)

	h1 := newHiveForTest()
	app1 := registerPersistentApp(h1, ch)
	go h1.Start()
	waitTilStareted(h1)

	cfg1 := h1.Config()

	h2 := newHiveForTest(PeerAddrs(cfg1.Addr))
	registerPersistentApp(h2, ch)
	go h2.Start()
	waitTilStareted(h2)

	h3 := newHiveForTest(PeerAddrs(cfg1.Addr))
	registerPersistentApp(h3, ch)
	go h3.Start()
	waitTilStareted(h3)

	h1.Emit(AppTestMsg(0))
	<-ch
	h1.Emit(AppTestMsg(0))
	id0 := <-ch

	b3 := findBee(app1.Name(), h3)
	if b3 == 0 {
		t.Fatalf("cannot find the bee on %v", h3)
	}

	_, err := app1.(*app).qee.sendCmdToBee(id0.Bee, cmdHandoff{
		To: b3,
	})
	if err != nil {
		t.Errorf("cannot handoff bee: %v", err)
	}
	h2.Emit(AppTestMsg(0))
	id1 := <-ch
	h3.Emit(AppTestMsg(0))
	id2 := <-ch
	if id1.Bee != b3 {
		t.Errorf("different bees want=%v got=%v", b3, id1.Bee)
	}
	if id1.Bee != id2.Bee {
		t.Errorf("different bees want=%v got=%v", id1.Bee, id2.Bee)
	}

	time.Sleep(cfg1.RaftElectTimeout())
	h1.Stop()
	h2.Stop()
	h3.Stop()
}

func TestReplicatedAppMigrateToFollower(t *testing.T) {
	ch := make(chan hiveAndBeeID)
	apps := make([]App, 3)

	h1 := newHiveForTest()
	apps[0] = registerPersistentApp(h1, ch)
	go h1.Start()
	waitTilStareted(h1)

	cfg1 := h1.Config()

	h2 := newHiveForTest(PeerAddrs(cfg1.Addr))
	apps[1] = registerPersistentApp(h2, ch)
	go h2.Start()
	waitTilStareted(h2)

	h3 := newHiveForTest(PeerAddrs(cfg1.Addr))
	apps[2] = registerPersistentApp(h3, ch)
	go h3.Start()
	waitTilStareted(h3)

	h1.Emit(AppTestMsg(0))
	<-ch
	h1.Emit(AppTestMsg(0))
	owner := <-ch

	b3 := findBee(apps[0].Name(), h3)
	if b3 == 0 {
		t.Fatalf("cannot find the persistent bee on hive %v", h3)
	}

	for try := 0; try < 3; try++ {
		_, err := apps[owner.Hive-1].(*app).qee.processCmd(cmdMigrate{
			Bee: owner.Bee,
			To:  h3.ID(),
		})
		if err != nil {
			t.Fatalf("cannot handoff bee: %v", err)
		}
		h2.Emit(AppTestMsg(0))
		owner = <-ch
		if owner.Bee == b3 {
			break
		}
	}

	if owner.Bee != b3 {
		t.Fatalf("different bees want=%v got=%v", b3, owner.Bee)
	}

	h3.Emit(AppTestMsg(0))
	next := <-ch
	if owner.Bee != next.Bee {
		t.Errorf("different bees want=%v got=%v", owner.Bee, next.Bee)
	}

	time.Sleep(cfg1.RaftElectTimeout())
	h1.Stop()
	h2.Stop()
	h3.Stop()
}

func TestReplicatedAppMigrateToNewHive(t *testing.T) {
	ch := make(chan hiveAndBeeID)

	h1 := newHiveForTest()
	app1 := registerPersistentApp(h1, ch)
	go h1.Start()
	waitTilStareted(h1)

	cfg1 := h1.Config()

	h2 := newHiveForTest(PeerAddrs(cfg1.Addr))
	registerPersistentApp(h2, ch)
	go h2.Start()
	waitTilStareted(h2)

	h3 := newHiveForTest(PeerAddrs(cfg1.Addr))
	registerPersistentApp(h3, ch)
	go h3.Start()
	waitTilStareted(h3)

	h1.Emit(AppTestMsg(0))
	<-ch
	h1.Emit(AppTestMsg(0))
	id0 := <-ch

	h4 := newHiveForTest(PeerAddrs(cfg1.Addr))
	registerPersistentApp(h4, ch)
	go h4.Start()
	waitTilStareted(h4)

	_, err := app1.(*app).qee.processCmd(cmdMigrate{
		Bee: id0.Bee,
		To:  h4.ID(),
	})
	if err != nil {
		t.Errorf("cannot handoff bee: %v", err)
	}
	h2.Emit(AppTestMsg(0))
	id1 := <-ch
	h3.Emit(AppTestMsg(0))
	id2 := <-ch

	b4 := findBee(app1.Name(), h4)
	if b4 == 0 {
		t.Fatalf("cannot find the bee on %v", h4)
	}

	if id1.Bee != b4 {
		t.Errorf("different bees want=%v got=%v", b4, id1.Bee)
	}
	if id1.Bee != id2.Bee {
		t.Errorf("different bees want=%v got=%v", id1.Bee, id2.Bee)
	}

	time.Sleep(cfg1.RaftElectTimeout())
	h1.Stop()
	h2.Stop()
	h3.Stop()
	h4.Stop()
}

func TestAppHTTP(t *testing.T) {
	h := hive{config: hiveConfig()}
	h.httpServer = newServer(&h)
	a := &app{
		name: "testapp",
		hive: &h,
	}
	a.HandleHTTPFunc("/test", func(w http.ResponseWriter, r *http.Request) {})
	go h.httpServer.ListenAndServe()
	time.Sleep(1 * time.Second)
	resp, err := http.Get(
		fmt.Sprintf("http://%s/apps/testapp/test", h.config.Addr))
	if err != nil {
		t.Error(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("reponse status: actual=%v want=200 Ok", resp.Status)
	}
}

func TestRuntimeMap(t *testing.T) {
	h := newHiveForTest()
	a := h.NewApp("RuntimeMap")

	go h.Start()
	defer h.Stop()
	waitTilStareted(h)

	cells := MappedCells{{"1", "11"}, {"1", "12"}, {"2", "21"}}
	rcv := func(msg Msg, ctx RcvContext) error {
		for _, c := range cells {
			ctx.Dict(c.Dict).Put(c.Key, []byte{})
		}
		return nil
	}
	mapped := RuntimeMap(rcv)(nil, a.(*app).qee)

	for _, rc := range cells {
		found := false
		for _, mc := range mapped {
			if rc == mc {
				found = true
			}
		}
		if !found {
			t.Fatalf("invalid mapped cells in runtime mapper: actual=%v want=%v",
				mapped, cells)
		}
	}
}

func findBee(a string, h Hive) uint64 {
	for _, b := range h.(*hive).registry.bees() {
		if b.App == a && b.Hive == h.ID() {
			return b.ID
		}
	}
	return 0
}
