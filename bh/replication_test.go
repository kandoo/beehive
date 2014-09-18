package bh

import "testing"

type hiveJoinedHandler struct {
	joined chan bool
}

func (h *hiveJoinedHandler) Rcv(msg Msg, ctx RcvContext) error {
	return nil
}

func (h *hiveJoinedHandler) Map(msg Msg, ctx MapContext) MappedCells {
	h.joined <- true
	return nil
}

func startHivesForReplicationTest(t *testing.T, addrs []string,
	preStart func(h Hive)) []Hive {

	maybeSkipRegistryTest(t)

	hiveJoinedCh := make(chan bool)
	hives := make([]Hive, len(addrs))
	for i, a := range addrs {
		hives[i] = hiveWithAddressForRegistryTests(a, t)
		hives[i].NewApp("joined").Handle(HiveJoined{}, &hiveJoinedHandler{
			joined: hiveJoinedCh,
		})
		preStart(hives[i])
		go hives[i].Start()
	}

	for _ = range addrs {
		for _ = range addrs {
			<-hiveJoinedCh
		}
	}

	return hives
}

func stopHivesForReplicationTest(hives []Hive) {
	for i := range hives {
		hives[i].Stop()
	}
}

func TestReplicationStrategy(t *testing.T) {
	hives := startHivesForReplicationTest(t,
		[]string{"127.0.0.1:32771", "127.0.0.1:32772"}, func(h Hive) {})

	slaves := hives[1].ReplicationStrategy().SelectSlaveHives(nil, 2)
	if len(slaves) != 1 {
		t.Errorf("Returned more slaves that asked: %+v", slaves)
	}

	if slaves[0] != hives[0].ID() {
		t.Errorf("Wrong slave selected %+v", hives[0].ID())
	}

	stopHivesForReplicationTest(hives)
}

type replicatedTestAppMsg int

type replicatedTestApp struct {
	rcvCh chan bool
}

func (h *replicatedTestApp) Rcv(msg Msg, ctx RcvContext) error {
	h.rcvCh <- true
	return nil
}

func (h *replicatedTestApp) Map(msg Msg, ctx MapContext) MappedCells {
	return MappedCells{{"D", Key("K")}}
}

func TestReplicatedBee(t *testing.T) {
	addrs := []string{"127.0.0.1:32771", "127.0.0.1:32772", "127.0.0.1:32773"}
	rcvCh := make(chan bool, len(addrs)*len(addrs))
	registerApp := func(h Hive) {
		app := h.NewApp("MyApp")
		app.Handle(replicatedTestAppMsg(0), &replicatedTestApp{rcvCh})
		app.SetReplicationFactor(len(addrs))
		app.SetFlags(AppFlagTransactional)
	}

	hives := startHivesForReplicationTest(t, addrs, registerApp)

	hives[0].Emit(replicatedTestAppMsg(0))
	<-rcvCh

	stopHivesForReplicationTest(hives)
	for _, b := range hives[0].(*hive).apps["MyApp"].qee.idToBees {
		colony := b.colony()
		if len(colony.Slaves) != len(addrs)-1 {
			t.Errorf("Incorrect number of slaves for MyApp: %+v", colony)
		}
	}

	for i := 1; i < len(addrs); i++ {
		for _, b := range hives[i].(*hive).apps["MyApp"].qee.idToBees {
			if localB, ok := b.(*localBee); ok && len(localB.txBuf) != 1 {
				t.Errorf("Incorrect number of transaction in slave: %+v", b.id())
			}
		}
	}
}
