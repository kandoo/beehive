package beehive

import (
	"strconv"
	"testing"
)

type qeeBenchHandler struct {
	last string
	done chan struct{}
}

func (h qeeBenchHandler) Map(msg Msg, ctx MapContext) MappedCells {
	return MappedCells{{"d", msg.Data().(string)}}
}

func (h qeeBenchHandler) Rcv(msg Msg, ctx RcvContext) error {
	if msg.Data().(string) == h.last {
		close(h.done)
	}
	return nil
}

func doBenchmarkQueenBeeCreation(b *testing.B, hiveN int) {
	b.StopTimer()

	done := make(chan struct{})

	handler := qeeBenchHandler{
		last: strconv.Itoa(b.N - 1),
		done: done,
	}

	var hives []Hive
	for i := 0; i < hiveN; i++ {
		cfg := DefaultCfg
		cfg.StatePath = "/tmp/bhbench-queen-single"
		removeState(cfg)
		cfg.Addr = newHiveAddrForTest()
		if i != 0 {
			cfg.PeerAddrs = []string{hives[0].(*hive).config.Addr}
		}
		h := NewHiveWithConfig(cfg)

		a := h.NewApp("qeeBenchApp")
		a.Handle("", handler)

		go h.Start()
		waitTilStareted(h)
		defer h.Stop()

		hives = append(hives, h)
	}

	msgs := make([]msgAndHandler, 0, b.N)
	for i := 0; i < b.N; i++ {
		msgs = append(msgs, msgAndHandler{
			msg:     &msg{MsgData: strconv.Itoa(i)},
			handler: handler,
		})
	}

	a, _ := hives[0].(*hive).app("qeeBenchApp")
	qee := a.qee

	b.StartTimer()

	batch := int(DefaultCfg.BatchSize)
	for i := 0; i < b.N/batch; i++ {
		from := i * batch
		to := from + batch
		if b.N < to {
			to = b.N
		}
		qee.handleMsgs(msgs[from:to])
	}

	b.StopTimer()

	<-done
}

func BenchmarkQueenBeeCreationSingle(b *testing.B) {
	doBenchmarkQueenBeeCreation(b, 1)
}

func BenchmarkQueenBeeCreationClustered(b *testing.B) {
	doBenchmarkQueenBeeCreation(b, 3)
}
