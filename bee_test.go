package beehive

import (
	"io/ioutil"
	"log"
	"testing"
	"time"

	"github.com/kandoo/beehive/state"
)

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
				StatePath: "/tmp/bhtest_bench_bee",
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
	bee.startNode()
	bee.becomeLeader()
	b.StartTimer()

	h := benchBeeHandler{data: []byte{1, 1, 1, 1}}
	for i := 0; i < b.N; i += bee.batchSize {
		mhs := make([]msgAndHandler, 0, bee.batchSize)
		for j := 0; j < bee.batchSize; j++ {
			mhs = append(mhs, msgAndHandler{
				msg:     &msg{},
				handler: h,
			})
		}
		bee.handleMsg(mhs)
	}

	b.StopTimer()
	bee.stopNode()
	time.Sleep(1 * time.Second)
}
