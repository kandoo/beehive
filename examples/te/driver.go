package main

import (
	"github.com/golang/glog"
	"github.com/soheilhy/actor/actor"
)

type FlowStat struct {
	Flow  Flow
	Bytes int
}

type SwitchState struct {
	Switch Switch
	Flows  []FlowStat
}

const (
	switchStateDict = "SwitchState"
)

type Driver struct {
	switches map[Switch]SwitchState
}

func NewDriver(startingSwitchId, numberOfSwitches int) *Driver {
	d := &Driver{make(map[Switch]SwitchState)}
	for i := 0; i < numberOfSwitches; i++ {
		sw := Switch(startingSwitchId + i)
		state := SwitchState{Switch: sw}
		state.Flows = append(state.Flows, FlowStat{Flow{1, 1, 2}, 100})
		d.switches[sw] = state
	}
	return d
}

func (d *Driver) Start(ctx actor.RecvContext) {
	for s, _ := range d.switches {
		ctx.Emit(SwitchJoined{s})
	}
}

func (d *Driver) Stop(ctx actor.RecvContext) {
}

func (d *Driver) Recv(m actor.Msg, ctx actor.RecvContext) {
	q := m.Data().(StatQuery)
	s, ok := d.switches[q.Switch]
	if !ok {
		glog.Fatalf("No switch stored in the driver: %+v", s)
	}

	for _, f := range s.Flows {
		glog.Infof("Emitting stat result for %#v", f)
		ctx.Emit(StatResult{q, f.Flow, 100})
	}
}

func (d *Driver) Map(m actor.Msg, ctx actor.Context) actor.MapSet {
	k := m.Data().(StatQuery).Switch.Key()
	return actor.MapSet{{switchStateDict, k}}
}