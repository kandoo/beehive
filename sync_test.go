package beehive

import (
	"fmt"
	"io/ioutil"
	"log"
	"testing"

	"github.com/kandoo/beehive/Godeps/_workspace/src/code.google.com/p/go.net/context"
)

func TestSync(t *testing.T) {
	cfg := DefaultCfg
	cfg.StatePath = "/tmp/bhtest-sync"
	cfg.Addr = newHiveAddrForTest()
	removeState(cfg)
	h := NewHiveWithConfig(cfg)

	app := h.NewApp("sync")
	type query string
	rcvf := func(msg Msg, ctx RcvContext) error {
		ctx.ReplyTo(msg, msg.Data().(query))
		return nil
	}
	mapf := func(msg Msg, ctx MapContext) MappedCells {
		return ctx.LocalMappedCells()
	}
	sync := NewSync(app)
	sync.HandleFunc(query(""), mapf, rcvf)

	go h.Start()
	defer h.Stop()

	req := query("test")
	res, err := sync.Process(context.Background(), req)
	if err != nil {
		t.Fatalf("error in process: %v", err)
	}
	if res != req {
		t.Errorf("sync.Process(%v) = %v; want=%v", req, res, req)
	}
}

func TestSyncCancel(t *testing.T) {
	type query string
	sync := &Sync{
		reqch: make(chan requestAndChan, 2048),
		done:  make(chan chan struct{}),
		reqs:  make(map[uint64]chan response),
	}
	req := query("test")
	ctx, ccl := context.WithCancel(context.Background())
	go ccl()
	_, err := sync.Process(ctx, req)
	if err == nil {
		t.Errorf("no error in process: %v", err)
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
	type query string
	sync := NewSync(app)
	sync.Handle(0, benchSyncHandler{})

	go h.Start()
	defer h.Stop()
	waitTilStareted(h)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		sync.Process(context.Background(), i)
	}
	b.StopTimer()
}

func ExampleSyncInstall() {
	type query string
	rcvf := func(msg Msg, ctx RcvContext) error {
		ctx.ReplyTo(msg, "hello "+msg.Data().(query))
		return nil
	}
	mapf := func(msg Msg, ctx MapContext) MappedCells {
		return ctx.LocalMappedCells()
	}

	hive := NewHive()
	app := hive.NewApp("sync-app")

	sync := NewSync(app)
	sync.HandleFunc(query(""), mapf, rcvf)

	go hive.Start()
	defer hive.Stop()

	result, err := sync.Process(context.Background(), query("your name"))
	if err != nil {
		fmt.Printf("error in sync: %v", err)
		return
	}
	fmt.Println(result)
}
