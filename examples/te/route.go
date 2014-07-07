package main

import (
	"github.com/golang/glog"
	"github.com/soheilhy/actor/actor"
)

const (
	topologyDict = "Topology"
)

type UpdateHandler struct{}

func (r *UpdateHandler) Recv(m actor.Msg, ctx actor.RecvContext) {
	glog.Infof("Received matrix update: %+v", m.Data().(MatrixUpdate))
}

func (r *UpdateHandler) Map(m actor.Msg, ctx actor.Context) actor.MapSet {
	return actor.MapSet{
		{matrixDict, actor.Key("0")},
		{topologyDict, actor.Key("0")},
	}
}

type TopologyHandler struct{}

func (t *TopologyHandler) Recv(m actor.Msg, ctx actor.Context) {
}

func (t *TopologyHandler) Map(m actor.Msg, ctx actor.Context) actor.MapSet {
	return actor.MapSet{{topologyDict, actor.Key("0")}}
}
