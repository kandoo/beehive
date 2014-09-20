package bh

import (
	"encoding/binary"
	"fmt"
	"testing"
	"time"
)

type TestFailureMessage int

func (m TestFailureMessage) Bytes() []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(m))
	return b
}

func (m *TestFailureMessage) Decode(b []byte) {
	*m = TestFailureMessage(binary.LittleEndian.Uint64(b))
}

func TestSlaveFailure(t *testing.T) {
	maybeSkipRegistryTest(t)

	addrs := []string{
		"127.0.0.1:32771",
		"127.0.0.1:32772",
		"127.0.0.1:32773",
		"127.0.0.1:32774",
	}

	ch := make(chan bool, 2)
	rcvF := func(msg Msg, ctx RcvContext) error {
		data := msg.Data().(TestFailureMessage)
		if data%10 == 0 {
			ch <- true
			return nil
		}

		dict := ctx.Dict("N")
		dict.Put("I", data.Bytes())
		ctx.Emit(data + 1)
		return nil
	}

	mapF := func(msg Msg, ctx MapContext) MappedCells {
		return MappedCells{{"N", "I"}}
	}

	hives := startHivesForReplicationTest(t, addrs[:3], func(h Hive) {
		app := h.NewApp("FailingApp")
		app.SetReplicationFactor(len(addrs) - 1)
		app.HandleFunc(TestFailureMessage(0), mapF, rcvF)
	})

	hives[0].Emit(TestFailureMessage(1))

	<-ch

	slaveCh := make(chan bool)
	slave := hiveWithAddressForRegistryTests(addrs[3], t)
	slave.NewApp("joined").Handle(HiveJoined{}, &hiveJoinedHandler{
		joined: slaveCh,
	})

	app := slave.NewApp("FailingApp")
	app.SetReplicationFactor(len(addrs) - 1)
	app.HandleFunc(TestFailureMessage(0), mapF, rcvF)

	go slave.Start()

	for _ = range addrs {
		<-slaveCh
	}

	hives[2].Stop()

	time.Sleep(1 * time.Second)

	hives[0].Emit(TestFailureMessage(1))

	time.Sleep(1 * time.Second)
	<-ch
	testFailureQee := slave.qees[msgType(TestFailureMessage(0))][0]
	fmt.Printf("%#v\n", testFailureQee.q)
	for _, b := range testFailureQee.q.idToBees {
		fmt.Printf("%#v\n", b)
	}

	hives[0].Stop()
	hives[1].Stop()
	slave.Stop()
}
