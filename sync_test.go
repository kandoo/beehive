package beehive

import (
	"fmt"
	"io/ioutil"
	"log"
	"testing"

	"github.com/kandoo/beehive/Godeps/_workspace/src/golang.org/x/net/context"
)

type query string

func TestSync(t *testing.T) {
	cfg := DefaultCfg
	cfg.StatePath = "/tmp/bhtest-sync"
	cfg.Addr = newHiveAddrForTest()
	removeState(cfg)
	h := NewHiveWithConfig(cfg)

	app := h.NewApp("sync")
	rcvf := func(msg Msg, ctx RcvContext) error {
		ctx.ReplyTo(msg, msg.Data().(query))
		return nil
	}
	mapf := func(msg Msg, ctx MapContext) MappedCells {
		return ctx.LocalMappedCells()
	}
	app.HandleFunc(query(""), mapf, rcvf)

	go h.Start()
	defer h.Stop()

	req := query("test")
	res, err := h.Sync(context.Background(), req)
	if err != nil {
		t.Fatalf("error in process: %v", err)
	}
	if res != req {
		t.Errorf("sync.Process(%v) = %v; want=%v", req, res, req)
	}
}

func TestSyncCancel(t *testing.T) {
	cfg := DefaultCfg
	cfg.StatePath = "/tmp/bhtest-sync"
	cfg.Addr = newHiveAddrForTest()
	removeState(cfg)
	h := NewHiveWithConfig(cfg)

	req := query("test")
	ctx, ccl := context.WithCancel(context.Background())
	go ccl()
	_, err := h.Sync(ctx, req)
	if err == nil {
		t.Errorf("no error in process: %v", err)
	}
}

func TestSyncDeferReply(t *testing.T) {
	cfg := DefaultCfg
	cfg.StatePath = "/tmp/bhtest-sync"
	cfg.Addr = newHiveAddrForTest()
	removeState(cfg)
	h := NewHiveWithConfig(cfg)

	app := h.NewApp("syncDeferReply")
	type reply string
	type deferred struct {
		Repliable
		Q query
	}
	deferredCh := make(chan struct{})

	deferf := func(msg Msg, ctx RcvContext) error {
		deferredCh <- struct{}{}
		r := ctx.DeferReply(msg)
		return ctx.Dict("sync").Put("reply", deferred{
			Repliable: r,
			Q:         msg.Data().(query),
		})
	}

	replyf := func(msg Msg, ctx RcvContext) error {
		v, err := ctx.Dict("sync").Get("reply")
		if err != nil {
			t.Fatalf("cannot decode reply: %v", err)
		}
		d := v.(deferred)
		d.Reply(ctx, d.Q)
		return nil
	}

	mapf := func(msg Msg, ctx MapContext) MappedCells {
		return ctx.LocalMappedCells()
	}

	app.HandleFunc(query(""), mapf, deferf)
	app.HandleFunc(reply(""), mapf, replyf)

	go h.Start()
	defer h.Stop()

	go func() {
		<-deferredCh
		h.Emit(reply(""))
	}()

	req := query("test")
	res, err := h.Sync(context.Background(), req)
	if err != nil {
		t.Fatalf("error in process: %v", err)
	}
	if res != req {
		t.Errorf("sync.Process(%v) = %v; want=%v", req, res, req)
	}
}

type benchSyncHandler struct{}

func (h benchSyncHandler) Rcv(msg Msg, ctx RcvContext) error {
	ctx.ReplyTo(msg, msg.Data().(int))
	return nil
}

func (h benchSyncHandler) Map(msg Msg, ctx MapContext) MappedCells {
	return ctx.LocalMappedCells()
}

func BenchmarkSync(b *testing.B) {
	log.SetOutput(ioutil.Discard)
	b.StopTimer()

	cfg := DefaultCfg
	cfg.StatePath = "/tmp/bhbench-sync"
	cfg.Addr = newHiveAddrForTest()
	removeState(cfg)
	h := NewHiveWithConfig(cfg)

	app := h.NewApp("sync")
	app.Handle(0, benchSyncHandler{})

	go h.Start()
	defer h.Stop()
	waitTilStareted(h)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		h.Sync(context.Background(), i)
	}
	b.StopTimer()
}

func ExampleSyncInstall() {
	rcvf := func(msg Msg, ctx RcvContext) error {
		ctx.ReplyTo(msg, "hello "+msg.Data().(query))
		return nil
	}
	mapf := func(msg Msg, ctx MapContext) MappedCells {
		return ctx.LocalMappedCells()
	}

	hive := NewHive()
	app := hive.NewApp("sync-app")
	app.HandleFunc(query(""), mapf, rcvf)

	go hive.Start()
	defer hive.Stop()

	result, err := hive.Sync(context.Background(), query("your name"))
	if err != nil {
		fmt.Printf("error in sync: %v", err)
		return
	}
	fmt.Println(result)
}
