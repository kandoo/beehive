package beehive

import (
	"fmt"
	"strconv"
	"testing"
)

func TestQueenMultipleKeys(t *testing.T) {
	cfg := newHiveConfigForTest()
	h := NewHiveWithConfig(cfg)

	ch := make(chan uint64)
	mapf := func(msg Msg, ctx MapContext) MappedCells {
		return MappedCells{{"D", msg.Data().(string)}, {"D", "K"}}
	}

	rcvf := func(msg Msg, ctx RcvContext) error {
		ch <- ctx.ID()
		return nil
	}

	a := h.NewApp("multikey")
	a.HandleFunc("", mapf, rcvf)

	l := 10
	for i := 0; i < l; i++ {
		h.Emit(fmt.Sprintf("test%d", i))
	}

	go h.Start()
	defer h.Stop()

	first := <-ch
	for i := 0; i < l-1; i++ {
		bee := <-ch
		if bee != first {
			t.Errorf("invalid bee receives the %d'th message: get=%d want=%d", i,
				bee, first)
		}
	}
}

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
		cfg := newHiveConfigForTest()
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
	batch := int(DefaultCfg.BatchSize)

	b.StartTimer()

	if b.N < batch {
		return
	}

	for i := 0; i <= b.N/batch; i++ {
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
