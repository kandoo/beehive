package bh

import "testing"

type joinedHandler struct {
	joined chan bool
}

func (h *joinedHandler) Rcv(msg Msg, ctx RcvContext) error {
	return nil
}

func (h *joinedHandler) Map(msg Msg, ctx MapContext) MappedCells {
	h.joined <- true
	return nil
}

func TestReplicationStrategy(t *testing.T) {
	h1 := hiveWithAddressForRegistryTests("127.0.0.1:32771", t)
	maybeSkipRegistryTest(h1, t)
	jCh1 := make(chan bool)
	go h1.Start(jCh1)

	h2 := hiveWithAddressForRegistryTests("127.0.0.1:32772", t)
	hiveJoinedCh := make(chan bool)
	h2.NewApp("joined").Handle(HiveJoined{}, &joinedHandler{
		joined: hiveJoinedCh,
	})
	jCh2 := make(chan bool)
	go h2.Start(jCh2)

	<-hiveJoinedCh
	<-hiveJoinedCh

	slaves := h2.ReplicationStrategy().SelectSlaveHives(MappedCells{}, 2)
	if len(slaves) != 1 {
		t.Errorf("Returned more slaves that asked: %+v", slaves)
	}

	if slaves[0] != h1.ID() {
		t.Errorf("Wrong slave selected %+v", h1.ID())
	}

	h1.Stop()
	<-jCh1

	h2.Stop()
	<-jCh2
}
