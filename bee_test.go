package beehive

import (
	"io/ioutil"
	"log"
	"testing"
	"time"

	"github.com/kandoo/beehive/bucket"
	"github.com/kandoo/beehive/state"
)

func TestDeferReply(t *testing.T) {
	type ping struct{}
	type pong struct{}
	type send struct{}

	ch := make(chan struct{})
	pingf := func(msg Msg, ctx RcvContext) error {
		ctx.Emit(ping{})
		if msg.IsUnicast() {
			close(ch)
		}
		return nil
	}

	pongf := func(msg Msg, ctx RcvContext) error {
		switch msg.Data().(type) {
		case ping:
			ctx.Emit(send{})
			r := ctx.DeferReply(msg)
			return ctx.Dict("reply").PutGob("def", &r)

		case send:
			var r Repliable
			if err := ctx.Dict("reply").GetGob("def", &r); err != nil {
				return err
			}
			r.Reply(ctx, pong{})
		}
		return nil
	}

	mapf := func(msg Msg, ctx MapContext) MappedCells {
		return ctx.LocalMappedCells()
	}

	cfg := DefaultCfg
	cfg.StatePath = "/tmp/bhtest-bee"
	cfg.Addr = newHiveAddrForTest()
	removeState(cfg)
	h := NewHiveWithConfig(cfg)

	app := h.NewApp("defer")
	app.HandleFunc(ping{}, mapf, pongf)
	app.HandleFunc(send{}, mapf, pongf)
	app.HandleFunc(pong{}, mapf, pingf)

	go h.Start()
	defer h.Stop()

	h.Emit(pong{})
	<-ch
}

func TestInRate(t *testing.T) {
	cfg := DefaultCfg
	cfg.StatePath = "/tmp/bhtest-bee-rate"
	cfg.Addr = newHiveAddrForTest()
	removeState(cfg)
	h := NewHiveWithConfig(cfg)

	type rateTestMsg struct{}
	ch := make(chan time.Time)
	rcvf := func(msg Msg, ctx RcvContext) error {
		ch <- time.Now()
		return nil
	}
	mapf := func(msg Msg, ctx MapContext) MappedCells {
		return ctx.LocalMappedCells()
	}

	app := h.NewApp("rate", LimitInRate(1*bucket.TPS, 1))
	app.HandleFunc(rateTestMsg{}, mapf, rcvf)

	go h.Start()
	defer h.Stop()

	h.Emit(rateTestMsg{})
	h.Emit(rateTestMsg{})

	t1 := <-ch
	t2 := <-ch
	if t2.Sub(t1) < 999*time.Millisecond {
		t.Errorf("the incoming message rate is higher than 1 tps: t1=%v t2=%v", t1,
			t2)
	}
}

func TestOutRate(t *testing.T) {
	cfg := DefaultCfg
	cfg.StatePath = "/tmp/bhtest-bee-rate"
	cfg.Addr = newHiveAddrForTest()
	removeState(cfg)
	h := NewHiveWithConfig(cfg)

	type outRateTestMsg struct{}
	type outRateTestStart struct{}

	mapf := func(msg Msg, ctx MapContext) MappedCells {
		return ctx.LocalMappedCells()
	}

	echof := func(msg Msg, ctx RcvContext) error {
		for i := 0; i < 2; i++ {
			ctx.ReplyTo(msg, msg.Data())
		}
		return nil
	}

	ch := make(chan time.Time)
	sinkf := func(msg Msg, ctx RcvContext) error {
		switch msg.Data().(type) {
		case outRateTestMsg:
			ch <- time.Now()
		case outRateTestStart:
			ctx.Emit(outRateTestMsg{})
		}
		return nil
	}

	eapp := h.NewApp("echo", LimitOutRate(1*bucket.TPS, 1))
	eapp.HandleFunc(outRateTestMsg{}, mapf, echof)

	sapp := h.NewApp("sink")
	sapp.HandleFunc(outRateTestMsg{}, mapf, sinkf)
	sapp.HandleFunc(outRateTestStart{}, mapf, sinkf)

	go h.Start()
	defer h.Stop()

	h.Emit(outRateTestStart{})

	t1 := <-ch
	t2 := <-ch

	if t2.Sub(t1) < 999*time.Millisecond {
		t.Errorf("output rate is higher than 1 tps: t1=%v t2=%v", t1, t2)
	}
}

type benchBeeHandler struct {
	data []byte
}

func (h benchBeeHandler) Rcv(msg Msg, ctx RcvContext) error {
	ctx.Dict("test").Put("k", h.data)
	return nil
}
func (h benchBeeHandler) Map(msg Msg, ctx MapContext) MappedCells {
	return nil
}

func BenchmarkBeePersistence(b *testing.B) {
	b.StopTimer()
	log.SetOutput(ioutil.Discard)

	bee := bee{
		beeID: 1,
		beeColony: Colony{
			Leader: 1,
		},
		hive: &hive{
			config: HiveConfig{
				StatePath:      "/tmp/bhtest_bench_bee",
				RaftTick:       100 * time.Millisecond,
				RaftHBTicks:    1,
				RaftElectTicks: 5,
			},
			collector: &noOpStatCollector{},
		},
		app: &app{
			name:  "test",
			flags: appFlagTransactional | appFlagPersistent,
		},
		stateL1:   state.NewTransactional(state.NewInMem()),
		dataCh:    newMsgChannel(b.N),
		batchSize: 1024,
	}
	removeState(bee.hive.config)
	bee.startNode()
	bee.becomeLeader()
	b.StartTimer()

	h := benchBeeHandler{data: []byte{1, 1, 1, 1}}
	mhs := make([]msgAndHandler, bee.batchSize)
	for j := 0; j < bee.batchSize; j++ {
		mhs[j] = msgAndHandler{
			msg:     &msg{},
			handler: h,
		}
	}
	for i := 0; i < b.N; i += bee.batchSize {
		bee.handleMsg(mhs)
	}

	b.StopTimer()
	bee.stopNode()
	time.Sleep(1 * time.Second)
}
