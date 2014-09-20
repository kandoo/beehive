package bh

import (
	"encoding/binary"
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

	const nMsgs = 10

	addrs := hiveAddrsForTest(4)

	ch := make(chan bool, 2)
	rcvF := func(msg Msg, ctx RcvContext) error {
		data := msg.Data().(TestFailureMessage)
		if data%nMsgs == 0 {
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
	slave := hiveWithAddressForTest(addrs[3], t)
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

	stopHives(hives[2])

	time.Sleep(1 * time.Second)
	hives[0].Emit(TestFailureMessage(1))
	<-ch
	time.Sleep(1 * time.Second)

	testFailureQee := slave.qees[msgType(TestFailureMessage(0))][0]

	if len(testFailureQee.q.idToBees) == 0 {
		t.Errorf("Did not created the new slave")
	}

	for _, b := range testFailureQee.q.idToBees {
		local, ok := b.(*localBee)
		if !ok {
			continue
		}

		if len(local.txBuf) != 2*(nMsgs-1) {
			t.Errorf("Slave does not have a correct number of transactions")
			continue
		}

		for i := 0; i < 2*(nMsgs-1); i++ {
			if local.txBuf[i].Seq != TxSeq(i+1) {
				t.Errorf("Incorrect transaction sequence: %d vs %d", local.txBuf[i].Seq,
					i+1)
			}
		}
	}

	stopHives(append(hives[:2], slave)...)
}
