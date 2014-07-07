package main

import "github.com/soheilhy/actor/actor"

type FlowStat struct {
	Flow  Flow
	Bytes int
}

type SwitchState struct {
	Switch Switch
	Flows  []FlowStat
}

type Driver struct {
	switches map[Switch]SwitchState
}

func NewDriver(startingSwitchId, numberOfSwitches int) *Driver {
	d := &Driver{make(map[Switch]SwitchState)}
	for i := 0; i < numberOfSwitches; i++ {
		sw := Switch(startingSwitchId + i)
		d.switches[sw] = SwitchState{Switch: sw}
	}
	return d
}

func (d *Driver) Start(ctx actor.RecvContext) {
}

func (d *Driver) Recv(m actor.Msg, ctx actor.RecvContext) {
}

func (d *Driver) Map(m actor.Msg, ctx actor.Context) actor.MapSet {
	return actor.MapSet{
		{matrixDict, actor.Key("0")},
		{topologyDict, actor.Key("0")},
	}
}
