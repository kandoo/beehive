package main

import (
	"fmt"
	"math/rand"

	"github.com/golang/glog"
	"github.com/kandoo/beehive/bh"
)

type FlowStat struct {
	Flow  Flow
	Bytes uint64
}

type SwitchState struct {
	Switch Switch
	Flows  []FlowStat
}

type FlowMod struct {
	Switch Switch
	Flow   Flow
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

func (d *Driver) Start(ctx bh.RcvContext) {
	for s, _ := range d.switches {
		ctx.Emit(SwitchJoined{s})
	}
}

func (d *Driver) Stop(ctx bh.RcvContext) {
}

func (d *Driver) Rcv(m bh.Msg, ctx bh.RcvContext) error {
	if m.From().AppName == "" {
		return nil
	}

	q, ok := m.Data().(StatQuery)
	if !ok {
		return nil
	}

	s, ok := d.switches[q.Switch]
	if !ok {
		return fmt.Errorf("No switch stored in the driver: %+v", s)
	}

	for i, f := range s.Flows {
		f.Bytes += uint64(rand.Intn(maxSpike))
		s.Flows[i] = f
		glog.V(2).Infof("Emitting stat result for %+v", f)
		ctx.Emit(StatResult{q, f.Flow, f.Bytes})
	}

	d.switches[q.Switch] = s
	return nil
}

func (d *Driver) Map(m bh.Msg, ctx bh.MapContext) bh.MappedCells {
	var k bh.Key
	switch d := m.Data().(type) {
	case StatQuery:
		k = d.Switch.Key()
	case FlowMod:
		k = d.Switch.Key()
	}
	return bh.MappedCells{{switchStateDict, k}}
}
