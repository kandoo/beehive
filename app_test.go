package beehive

import (
	"fmt"
	"net/http"
	"testing"
	"time"
)

type AppTestMsg int

func TestPersistentApp(t *testing.T) {
	cfg := DefaultCfg
	cfg.StatePath = "/tmp/bhtest"
	cfg.Addr = newHiveAddrForTest()
	removeState(cfg)
	h := NewHiveWithConfig(cfg)

	app := h.NewApp("persistent", AppPersistent(3))
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

func registerPersistentApp(h Hive, ch chan uint64) App {
	app := h.NewApp("persistent", AppPersistent(3))
	mf := func(msg Msg, ctx MapContext) MappedCells {
		return MappedCells{{"D", "0"}}
	}
	rf := func(msg Msg, ctx RcvContext) error {
		ctx.Dict("Test").Put("K", []byte{})
		ch <- ctx.ID()
		return nil
	}
	app.HandleFunc(AppTestMsg(0), mf, rf)
	return app
}

func TestReplicatedApp(t *testing.T) {
	ch := make(chan uint64)

	cfg1 := DefaultCfg
	cfg1.StatePath = "/tmp/bhtest1"
	cfg1.Addr = newHiveAddrForTest()
	removeState(cfg1)
	h1 := NewHiveWithConfig(cfg1)
	registerPersistentApp(h1, ch)
	go h1.Start()
	waitTilStareted(h1)

	cfg2 := DefaultCfg
	cfg2.StatePath = "/tmp/bhtest2"
	cfg2.Addr = newHiveAddrForTest()
	cfg2.PeerAddrs = []string{cfg1.Addr}
	removeState(cfg2)
	h2 := NewHiveWithConfig(cfg2)
	registerPersistentApp(h2, ch)
	go h2.Start()
	waitTilStareted(h2)

	cfg3 := DefaultCfg
	cfg3.StatePath = "/tmp/bhtest3"
	cfg3.Addr = newHiveAddrForTest()
	cfg3.PeerAddrs = []string{cfg1.Addr}
	removeState(cfg3)
	h3 := NewHiveWithConfig(cfg3)
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
	ch := make(chan uint64)

	cfg1 := DefaultCfg
	cfg1.StatePath = "/tmp/bhtest1"
	cfg1.Addr = newHiveAddrForTest()
	removeState(cfg1)
	h1 := NewHiveWithConfig(cfg1)
	registerPersistentApp(h1, ch)
	go h1.Start()
	waitTilStareted(h1)

	cfg2 := DefaultCfg
	cfg2.StatePath = "/tmp/bhtest2"
	cfg2.Addr = newHiveAddrForTest()
	cfg2.PeerAddrs = []string{cfg1.Addr}
	removeState(cfg2)
	h2 := NewHiveWithConfig(cfg2)
	registerPersistentApp(h2, ch)
	go h2.Start()
	waitTilStareted(h2)

	cfg3 := DefaultCfg
	cfg3.StatePath = "/tmp/bhtest3"
	cfg3.Addr = newHiveAddrForTest()
	cfg3.PeerAddrs = []string{cfg1.Addr}
	removeState(cfg3)
	h3 := NewHiveWithConfig(cfg3)
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
		t.Errorf("different bees want=%v given=%v", id1, id2)
	}

	time.Sleep(elect)
	h2.Stop()
	h3.Stop()
}

func TestReplicatedAppHandoff(t *testing.T) {
	ch := make(chan uint64)

	cfg1 := DefaultCfg
	cfg1.StatePath = "/tmp/bhtest1"
	cfg1.Addr = newHiveAddrForTest()
	removeState(cfg1)
	h1 := NewHiveWithConfig(cfg1)
	app1 := registerPersistentApp(h1, ch)
	go h1.Start()
	waitTilStareted(h1)

	cfg2 := DefaultCfg
	cfg2.StatePath = "/tmp/bhtest2"
	cfg2.Addr = newHiveAddrForTest()
	cfg2.PeerAddrs = []string{cfg1.Addr}
	removeState(cfg2)
	h2 := NewHiveWithConfig(cfg2)
	registerPersistentApp(h2, ch)
	go h2.Start()
	waitTilStareted(h2)

	cfg3 := DefaultCfg
	cfg3.StatePath = "/tmp/bhtest3"
	cfg3.Addr = newHiveAddrForTest()
	cfg3.PeerAddrs = []string{cfg1.Addr}
	removeState(cfg3)
	h3 := NewHiveWithConfig(cfg3)
	registerPersistentApp(h3, ch)
	go h3.Start()
	waitTilStareted(h3)

	h1.Emit(AppTestMsg(0))
	<-ch
	h1.Emit(AppTestMsg(0))
	id0 := <-ch

	_, err := app1.(*app).qee.sendCmdToBee(id0, cmdHandoff{
		To: 3,
	})
	if err != nil {
		t.Errorf("cannot handoff bee: %v", err)
	}
	h2.Emit(AppTestMsg(0))
	id1 := <-ch
	h3.Emit(AppTestMsg(0))
	id2 := <-ch
	if id1 != 3 {
		t.Errorf("different bees want=3 given=%v", id1)
	}
	if id1 != id2 {
		t.Errorf("different bees want=%v given=%v", id1, id2)
	}

	time.Sleep(cfg1.RaftElectTimeout())
	h1.Stop()
	h2.Stop()
	h3.Stop()
}

func TestReplicatedAppMigrateToFollower(t *testing.T) {
	ch := make(chan uint64)

	cfg1 := DefaultCfg
	cfg1.StatePath = "/tmp/bhtest1"
	cfg1.Addr = newHiveAddrForTest()
	removeState(cfg1)
	h1 := NewHiveWithConfig(cfg1)
	app1 := registerPersistentApp(h1, ch)
	go h1.Start()
	waitTilStareted(h1)

	cfg2 := DefaultCfg
	cfg2.StatePath = "/tmp/bhtest2"
	cfg2.Addr = newHiveAddrForTest()
	cfg2.PeerAddrs = []string{cfg1.Addr}
	removeState(cfg2)
	h2 := NewHiveWithConfig(cfg2)
	registerPersistentApp(h2, ch)
	go h2.Start()
	waitTilStareted(h2)

	cfg3 := DefaultCfg
	cfg3.StatePath = "/tmp/bhtest3"
	cfg3.Addr = newHiveAddrForTest()
	cfg3.PeerAddrs = []string{cfg1.Addr}
	removeState(cfg3)
	h3 := NewHiveWithConfig(cfg3)
	registerPersistentApp(h3, ch)
	go h3.Start()
	waitTilStareted(h3)

	h1.Emit(AppTestMsg(0))
	<-ch
	h1.Emit(AppTestMsg(0))
	id0 := <-ch

	owner := id0
	for try := 0; try < 3; try++ {
		_, err := app1.(*app).qee.processCmd(cmdMigrate{
			Bee: owner,
			To:  h3.ID(),
		})
		if err != nil {
			t.Fatalf("cannot handoff bee: %v", err)
		}
		h2.Emit(AppTestMsg(0))
		owner = <-ch
		if owner == 3 {
			break
		}
	}

	if owner != 3 {
		t.Fatalf("different bees want=3 given=%v", owner)
	}

	h3.Emit(AppTestMsg(0))
	next := <-ch
	if owner != next {
		t.Errorf("different bees want=%v given=%v", owner, next)
	}

	time.Sleep(cfg1.RaftElectTimeout())
	h1.Stop()
	h2.Stop()
	h3.Stop()
}

func TestReplicatedAppMigrateToNewHive(t *testing.T) {
	ch := make(chan uint64)

	cfg1 := DefaultCfg
	cfg1.StatePath = "/tmp/bhtest1"
	cfg1.Addr = newHiveAddrForTest()
	removeState(cfg1)
	h1 := NewHiveWithConfig(cfg1)
	app1 := registerPersistentApp(h1, ch)
	go h1.Start()
	waitTilStareted(h1)

	cfg2 := DefaultCfg
	cfg2.StatePath = "/tmp/bhtest2"
	cfg2.Addr = newHiveAddrForTest()
	cfg2.PeerAddrs = []string{cfg1.Addr}
	removeState(cfg2)
	h2 := NewHiveWithConfig(cfg2)
	registerPersistentApp(h2, ch)
	go h2.Start()
	waitTilStareted(h2)

	cfg3 := DefaultCfg
	cfg3.StatePath = "/tmp/bhtest3"
	cfg3.Addr = newHiveAddrForTest()
	cfg3.PeerAddrs = []string{cfg1.Addr}
	removeState(cfg3)
	h3 := NewHiveWithConfig(cfg3)
	registerPersistentApp(h3, ch)
	go h3.Start()
	waitTilStareted(h3)

	h1.Emit(AppTestMsg(0))
	<-ch
	h1.Emit(AppTestMsg(0))
	id0 := <-ch

	cfg4 := DefaultCfg
	cfg4.StatePath = "/tmp/bhtest4"
	cfg4.Addr = newHiveAddrForTest()
	cfg4.PeerAddrs = []string{cfg1.Addr}
	removeState(cfg4)
	h4 := NewHiveWithConfig(cfg4)
	registerPersistentApp(h4, ch)
	go h4.Start()
	waitTilStareted(h4)

	_, err := app1.(*app).qee.processCmd(cmdMigrate{
		Bee: id0,
		To:  h4.ID(),
	})
	if err != nil {
		t.Errorf("cannot handoff bee: %v", err)
	}
	h2.Emit(AppTestMsg(0))
	id1 := <-ch
	h3.Emit(AppTestMsg(0))
	id2 := <-ch
	if id1 != 4 {
		t.Errorf("different bees want=4 given=%v", id1)
	}
	if id1 != id2 {
		t.Errorf("different bees want=%v given=%v", id1, id2)
	}

	time.Sleep(cfg1.RaftElectTimeout())
	h1.Stop()
	h2.Stop()
	h3.Stop()
	h4.Stop()
}

func TestAppHTTP(t *testing.T) {
	h := hive{}
	addr := newHiveAddrForTest()
	h.server = newServer(&h, addr)
	a := &app{
		name: "testapp",
		hive: &h,
	}
	a.HandleHTTPFunc("/test", func(w http.ResponseWriter, r *http.Request) {})
	go h.server.ListenAndServe()
	resp, err := http.Get(fmt.Sprintf("http://%s/apps/testapp/test", addr))
	if err != nil {
		t.Error(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("reponse status: actual=%v want=200 Ok", resp.Status)
	}
}
