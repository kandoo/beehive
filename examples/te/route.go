package main

import (
	"github.com/golang/glog"
	"github.com/soheilhy/beehive/bh"
)

const (
	topologyDict = "Topology"
)

type UpdateHandler struct{}

func (r *UpdateHandler) Recv(m bh.Msg, ctx bh.RecvContext) {
	if m.From().ActorName == "" {
		return
	}

	u := m.Data().(MatrixUpdate)
	glog.Infof("Received matrix update: %+v", u)
	ctx.Emit(FlowMod{Switch: u.Switch})
}

func (r *UpdateHandler) Map(m bh.Msg, ctx bh.Context) bh.MapSet {
	return bh.MapSet{
		{matrixDict, bh.Key("0")},
		{topologyDict, bh.Key("0")},
	}
}

type TopologyHandler struct{}

func (t *TopologyHandler) Recv(m bh.Msg, ctx bh.RecvContext) {
}

func (t *TopologyHandler) Map(m bh.Msg, ctx bh.Context) bh.MapSet {
	return bh.MapSet{{topologyDict, bh.Key("0")}}
}
