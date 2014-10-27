package main

import (
	"github.com/golang/glog"
	bh "github.com/kandoo/beehive"
)

const (
	topologyDict = "Topology"
)

type UpdateHandler struct{}

func (r *UpdateHandler) Rcv(m bh.Msg, ctx bh.RcvContext) error {
	if m.NoReply() {
		return nil
	}

	u := m.Data().(MatrixUpdate)
	glog.Infof("Received matrix update: %+v", u)
	ctx.Emit(FlowMod{Switch: u.Switch})
	return nil
}

func (r *UpdateHandler) Map(m bh.Msg, ctx bh.MapContext) bh.MappedCells {
	return bh.MappedCells{
		{matrixDict, "0"},
		{topologyDict, "0"},
	}
}

type TopologyHandler struct{}

func (t *TopologyHandler) Rcv(m bh.Msg, ctx bh.RcvContext) error {
	return nil
}

func (t *TopologyHandler) Map(m bh.Msg, ctx bh.MapContext) bh.MappedCells {
	return bh.MappedCells{{topologyDict, "0"}}
}
