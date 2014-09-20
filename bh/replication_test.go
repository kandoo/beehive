package bh

import "testing"

func TestReplicationStrategy(t *testing.T) {
	addrs := hiveAddrsForTest(2)
	hives := startHivesForReplicationTest(t, addrs, func(h Hive) {})

	slaves := hives[1].ReplicationStrategy().SelectSlaveHives(nil, 2)
	if len(slaves) != 1 {
		t.Errorf("Returned more slaves that asked: %+v", slaves)
	}

	if slaves[0] != hives[0].ID() {
		t.Errorf("Wrong slave selected %+v", hives[0].ID())
	}

	stopHives(hives...)
}

type replicatedTestAppMsg int

type replicatedTestApp struct {
	rcvCh chan bool
}

func (h *replicatedTestApp) Rcv(msg Msg, ctx RcvContext) error {
	ctx.Dict("replicatedTestAppD").Put("K", Value("V"))
	ctx.CommitTx()
	h.rcvCh <- true
	return nil
}

func (h *replicatedTestApp) Map(msg Msg, ctx MapContext) MappedCells {
	return MappedCells{{"replicatedTestAppD", Key("K")}}
}

func TestReplicatedBee(t *testing.T) {
	addrs := hiveAddrsForTest(3)
	rcvCh := make(chan bool, len(addrs)*len(addrs))
	hives := startHivesForReplicationTest(t, addrs, func(h Hive) {
		app := h.NewApp("TestReplicationApp")
		app.Handle(replicatedTestAppMsg(0), &replicatedTestApp{rcvCh})
		app.SetReplicationFactor(len(addrs))
		app.SetFlags(AppFlagTransactional)
	})

	hives[0].Emit(replicatedTestAppMsg(0))
	<-rcvCh

	stopHives(hives...)
	for _, b := range hives[0].(*hive).apps["TestReplicationApp"].qee.idToBees {
		colony := b.colony()
		if len(colony.Slaves) != len(addrs)-1 {
			t.Errorf("Incorrect number of slaves for the app: %+v", colony)
		}
	}

	for i := 1; i < len(addrs); i++ {
		for _, b := range hives[i].(*hive).apps["TestReplicationApp"].qee.idToBees {
			if localB, ok := b.(*localBee); ok && len(localB.txBuf) != 1 {
				t.Errorf("Expected one transaction in slave %+v: %v vs 1",
					b.id(), len(localB.txBuf))
			}
		}
	}
}
