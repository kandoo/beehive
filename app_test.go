package beehive

import "testing"

type AppTestMsg int

func TestPersistentApp(t *testing.T) {
	cfg := DefaultCfg
	cfg.StatePath = "/tmp/bhtest"
	cfg.Addr = "127.0.0.1:7767"
	defer removeState(cfg)
	h := NewHiveWithConfig(cfg)

	app := h.NewApp("persistent")
	app.SetFlags(AppFlagTransactional | AppFlagPersistent)
	app.SetReplicationFactor(3)
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

func registerPersistentApp(h Hive, ch chan struct{}) {
	app := h.NewApp("persistent")
	app.SetFlags(AppFlagTransactional | AppFlagPersistent)
	app.SetReplicationFactor(3)
	mf := func(msg Msg, ctx MapContext) MappedCells {
		return ctx.LocalMappedCells()
	}
	rf := func(msg Msg, ctx RcvContext) error {
		ctx.Dict("Test").Put("K", []byte{})
		ch <- struct{}{}
		return nil
	}
	app.HandleFunc(AppTestMsg(0), mf, rf)
}

func TestReplicatedApp(t *testing.T) {
	ch := make(chan struct{})

	cfg1 := DefaultCfg
	cfg1.StatePath = "/tmp/bhtest1"
	cfg1.Addr = "127.0.0.1:7767"
	defer removeState(cfg1)
	h1 := NewHiveWithConfig(cfg1)
	registerPersistentApp(h1, ch)
	go h1.Start()
	waitTilStareted(h1)

	cfg2 := DefaultCfg
	cfg2.StatePath = "/tmp/bhtest2"
	cfg2.Addr = "127.0.0.1:7777"
	cfg2.PeerAddrs = []string{"127.0.0.1:7767"}
	defer removeState(cfg2)
	h2 := NewHiveWithConfig(cfg2)
	registerPersistentApp(h2, ch)
	go h2.Start()
	waitTilStareted(h2)

	cfg3 := DefaultCfg
	cfg3.StatePath = "/tmp/bhtest3"
	cfg3.Addr = "127.0.0.1:7787"
	cfg3.PeerAddrs = []string{"127.0.0.1:7767"}
	defer removeState(cfg3)
	h3 := NewHiveWithConfig(cfg3)
	registerPersistentApp(h3, ch)
	go h3.Start()
	waitTilStareted(h3)

	h1.Emit(AppTestMsg(0))
	<-ch
	h1.Emit(AppTestMsg(0))
	<-ch

	h1.Stop()
	h2.Stop()
	h3.Stop()
}
