package beehive

import (
	"encoding/binary"
	"flag"
	"io/ioutil"
	"log"
	"runtime"
	"strconv"
	"testing"
	"time"
)

var (
	benchmarkEndToEndBees = flag.Int("bench.e2e.bees", runtime.NumCPU(),
		"number of bees in end-to-end benchmarks")
)

func benchmarkEndToEnd(b *testing.B, name string, hives int, emittingHive int,
	handler Handler, app ...AppOption) {

	// Warm up.
	b.StopTimer()

	log.SetOutput(ioutil.Discard)
	bees := *benchmarkEndToEndBees
	kch := make(chan benchKill)
	var hs []Hive
	for i := 0; i < hives; i++ {
		cfg := DefaultCfg
		cfg.StatePath = "/tmp/bhbench-e2e-" + name + strconv.Itoa(i)
		removeState(cfg)
		cfg.Addr = newHiveAddrForTest()
		if i > 0 {
			cfg.PeerAddrs = []string{hs[0].(*hive).config.Addr}
		}
		h := NewHiveWithConfig(cfg)

		a := h.NewApp("handler", app...)
		a.Handle(BenchMsg(0), handler)
		a.Handle(benchKill{}, benchKillHandler{ch: kch})

		go h.Start()
		waitTilStareted(h)
		hs = append(hs, h)
	}

	mainHive := hs[0]
	for i := 1; i <= bees; i++ {
		mainHive.Emit(BenchMsg(i))
		mainHive.Emit(benchKill{BenchMsg: BenchMsg(i)})
		<-kch
	}

	b.StartTimer()
	emitting := hs[emittingHive]
	for i := 1; i <= bees; i++ {
		for j := 0; j < b.N/bees; j++ {
			emitting.Emit(BenchMsg(i))
		}
		emitting.Emit(benchKill{BenchMsg: BenchMsg(i)})
	}
	for i := 0; i < bees; i++ {
		<-kch
	}
	b.StopTimer()

	// Grace period for raft.
	time.Sleep(time.Duration(len(hs)) * time.Second)
}

func BenchmarkEndToEndTransientNoOp(b *testing.B) {
	benchmarkEndToEnd(b, "tx-noop", 1, 0, benchNoOpHandler{},
		NonTransactional())
}

func BenchmarkEndToEndTransientBytes(b *testing.B) {
	benchmarkEndToEnd(b, "tx-bytes", 1, 0, benchBytesHandler{},
		NonTransactional())
}

func BenchmarkEndToEndTransientGob(b *testing.B) {
	benchmarkEndToEnd(b, "tx-gob", 1, 0, benchGobHandler{}, NonTransactional())
}

func BenchmarkEndToEndTransactionalNoOp(b *testing.B) {
	benchmarkEndToEnd(b, "tx-noop", 1, 0, benchNoOpHandler{}, Transactional())
}

func BenchmarkEndToEndTransactionalBytes(b *testing.B) {
	benchmarkEndToEnd(b, "tx-bytes", 1, 0, benchBytesHandler{},
		Transactional())
}

func BenchmarkEndToEndTransactionalGob(b *testing.B) {
	benchmarkEndToEnd(b, "tx-gob", 1, 0, benchGobHandler{}, Transactional())
}

func BenchmarkEndToEndPersistentNoOp(b *testing.B) {
	benchmarkEndToEnd(b, "p-noop", 1, 0, benchNoOpHandler{}, Persistent(1))
}

func BenchmarkEndToEndPersistentBytes(b *testing.B) {
	benchmarkEndToEnd(b, "p-bytes", 1, 0, benchBytesHandler{}, Persistent(1))
}

func BenchmarkEndToEndPersistentGob(b *testing.B) {
	benchmarkEndToEnd(b, "p-gob", 1, 0, benchGobHandler{}, Persistent(1))
}

func BenchmarkEndToEndReplicatedNoOp(b *testing.B) {
	benchmarkEndToEnd(b, "r-noop", 3, 0, benchNoOpHandler{}, Persistent(3))
}

func BenchmarkEndToEndReplicatedBytes(b *testing.B) {
	benchmarkEndToEnd(b, "r-bytes", 3, 0, benchBytesHandler{}, Persistent(3))
}

func BenchmarkEndToEndReplicatedGob(b *testing.B) {
	benchmarkEndToEnd(b, "r-gob", 3, 0, benchGobHandler{}, Persistent(3))
}

func BenchmarkEndToEndRemoteTransactionalNoOp(b *testing.B) {
	benchmarkEndToEnd(b, "rt-noop", 3, 2, benchNoOpHandler{}, Transactional())
}

func BenchmarkEndToEndRemoteTransactionalBytes(b *testing.B) {
	benchmarkEndToEnd(b, "rt-bytes", 3, 2, benchBytesHandler{},
		Transactional())
}

func BenchmarkEndToEndRemoteTransactionalGob(b *testing.B) {
	benchmarkEndToEnd(b, "rt-gob", 3, 2, benchGobHandler{}, Transactional())
}

func BenchmarkEndToEndRemotePersistentNoOp(b *testing.B) {
	benchmarkEndToEnd(b, "rp-noop", 3, 2, benchNoOpHandler{}, Persistent(3))
}

func BenchmarkEndToEndRemotePersistentBytes(b *testing.B) {
	benchmarkEndToEnd(b, "rp-bytes", 3, 2, benchBytesHandler{}, Persistent(3))
}

func BenchmarkEndToEndRemotePersistentGob(b *testing.B) {
	benchmarkEndToEnd(b, "rp-gob", 3, 2, benchGobHandler{}, Persistent(3))
}

type BenchMsg int

func (m BenchMsg) key() string {
	return strconv.Itoa(int(m))
}

func keyForTestBenchMsg(m Msg) string {
	return m.Data().(BenchMsg).key()
}

const (
	benchDict   = "dict"
	benchShards = 16
)

type benchNoOpHandler struct{}

func (h benchNoOpHandler) Rcv(msg Msg, ctx RcvContext) error {
	return nil
}

func (h benchNoOpHandler) Map(msg Msg, ctx MapContext) MappedCells {
	return benchMap(msg, ctx)
}

type benchBytesHandler struct{}

func (h benchBytesHandler) Rcv(msg Msg, ctx RcvContext) error {
	dict := ctx.Dict(benchDict)
	k := keyForTestBenchMsg(msg)
	v, err := dict.Get(k)
	cnt := uint32(0)
	if err == nil {
		cnt = binary.LittleEndian.Uint32(v)
	}
	cnt++
	v = make([]byte, 4)
	binary.LittleEndian.PutUint32(v, cnt)
	return dict.Put(k, v)
}

func (h benchBytesHandler) Map(msg Msg, ctx MapContext) MappedCells {
	return benchMap(msg, ctx)
}

type benchGobHandler struct{}

func (h benchGobHandler) Rcv(msg Msg, ctx RcvContext) error {
	dict := ctx.Dict(benchDict)
	k := keyForTestBenchMsg(msg)
	cnt := uint32(0)
	dict.GetGob(k, &cnt)
	cnt++
	return dict.PutGob(k, &cnt)
}

func (h benchGobHandler) Map(msg Msg, ctx MapContext) MappedCells {
	return benchMap(msg, ctx)
}

func benchMap(msg Msg, ctx MapContext) MappedCells {
	return MappedCells{{benchDict, keyForTestBenchMsg(msg)}}
}

type benchKill struct {
	BenchMsg
}

type benchKillHandler struct {
	ch chan benchKill
}

func (h benchKillHandler) Rcv(msg Msg, ctx RcvContext) error {
	h.ch <- benchKill{}
	return nil
}

func (h benchKillHandler) Map(msg Msg, ctx MapContext) MappedCells {
	return MappedCells{{benchDict, msg.Data().(benchKill).key()}}
}
