package main

import (
	"github.com/kandoo/beehive"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/golang/glog"
)

const (
	topologyDict = "Topology"
)

type UpdateHandler struct{}

func (h *UpdateHandler) Rcv(m beehive.Msg, ctx beehive.RcvContext) error {
	if m.NoReply() {
		return nil
	}

	u := m.Data().(MatrixUpdate)
	glog.Infof("Received matrix update: %+v", u)
	ctx.Emit(FlowMod{Switch: u.Switch})
	return nil
}

func (h *UpdateHandler) Map(m beehive.Msg,
	ctx beehive.MapContext) beehive.MappedCells {

	return beehive.MappedCells{
		{matrixDict, "0"},
		{topologyDict, "0"},
	}
}

type TopologyHandler struct{}

func (h *TopologyHandler) Rcv(m beehive.Msg, ctx beehive.RcvContext) error {
	return nil
}

func (h *TopologyHandler) Map(m beehive.Msg,
	ctx beehive.MapContext) beehive.MappedCells {

	return beehive.MappedCells{{topologyDict, "0"}}
}
