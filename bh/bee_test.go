package bh

import (
	"testing"
	"time"
)

type SnoozeTestMsg int

func TestSnooze(t *testing.T) {
	h := NewHive()
	app := h.NewApp("SnoozingTestApp")

	mapF := func(msg Msg, ctx MapContext) MappedCells {
		return MappedCells{{"D", "K"}}
	}

	ch := make(chan bool)
	i := 0
	rcvF := func(msg Msg, ctx RcvContext) error {
		ch <- true
		i++
		if i == 1 {
			ctx.Snooze(time.Millisecond)
		}
		return nil
	}

	app.HandleFunc(SnoozeTestMsg(0), mapF, rcvF)

	h.Emit(SnoozeTestMsg(0))

	go h.Start()

	<-ch
	select {
	case <-ch:
	case <-time.After(10 * time.Millisecond):
		t.Errorf("Did not receive the snoozed message.")
	}

	h.Stop()
}

func TestColony(t *testing.T) {
	id := BeeID{
		HiveID:  "test",
		AppName: "app",
		ID:      1,
	}

	col := BeeColony{
		Master:     id,
		Generation: 1,
	}

	if col.AddSlave(id) {
		t.Errorf("Add slave allows the master to be a slave: %v", col)
	}

	id.ID++
	if !col.AddSlave(id) {
		t.Errorf("Cannot add slave %v to colony %v", id, col)
	}

	if col.AddSlave(id) {
		t.Errorf("Able to add a duplicate slave %v to %v", id, col)
	}

	id.ID++
	if !col.AddSlave(id) {
		t.Errorf("Cannot add slave %v to colony %v", id, col)
	}

	ccol := col.DeepCopy()

	if len(ccol.Slaves) != len(col.Slaves) {
		t.Errorf("Invalid slaves in the deep copied colony %v", ccol.Slaves)
	}

	for i := range col.Slaves {
		if ccol.Slaves[i] != col.Slaves[i] {
			t.Errorf("Deep copied colony %v is not the same as %v", ccol, col)
		}
	}
}
