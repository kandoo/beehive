package main

import (
	"strconv"
	"time"

	"github.com/golang/glog"
	"github.com/soheilhy/actor/actor"
)

type IPV4 uint32
type Switch uint64

func (s Switch) String() string {
	return strconv.FormatUint(uint64(s), 10)
}

func (s Switch) Key() actor.Key {
	return actor.Key(s.String())
}

type Flow struct {
	SrcIpV4 IPV4
	DstIpV4 IPV4
	OutPort uint32
}

type StatQuery struct {
	Switch Switch
}

type StatResult struct {
	StatQuery
	Flow  Flow
	Bytes uint64
}

type MatrixUpdate StatResult

type SwitchStats map[Flow]uint64

const (
	matrixDict = "Matrix"
)

type Collector struct {
	delta  uint64
	poller *Poller
}

func (c *Collector) Recv(m actor.Msg, ctx actor.RecvContext) {
	res := m.Data().(StatResult)
	glog.Infof("Stat results: %#v", res)
	matrix := ctx.Dict(matrixDict)
	key := res.Switch.Key()
	sw, ok := matrix.Get(key)
	if !ok {
		glog.Errorf("No such switch in matrix: %+v", res)
		return
	}

	c.poller.query <- StatQuery{res.Switch}

	stat, ok := sw.(SwitchStats)[res.Flow]
	sw.(SwitchStats)[res.Flow] = res.Bytes

	if !ok || res.Bytes-stat > c.delta {
		ctx.Emit(MatrixUpdate(res))
	}
}

func (c *Collector) Map(m actor.Msg, ctx actor.Context) actor.MapSet {
	return actor.MapSet{{matrixDict, m.Data().(StatResult).Switch.Key()}}
}

type Poller struct {
	quit chan chan bool

	timeout time.Duration

	query    chan StatQuery
	switches map[Switch]bool
}

func NewPoller(timeout time.Duration) *Poller {
	return &Poller{
		quit:     make(chan chan bool),
		timeout:  timeout,
		query:    make(chan StatQuery),
		switches: make(map[Switch]bool),
	}
}

func (p *Poller) Start(ctx actor.RecvContext) {
	for {
		select {
		case q := <-p.query:
			p.switches[q.Switch] = true
		case ch := <-p.quit:
			ch <- true
			return
		case <-time.After(p.timeout):
			for s, ok := range p.switches {
				if !ok {
					continue
				}
				ctx.Emit(StatQuery{s})
				p.switches[s] = false
				glog.Infof("Queried switch: %+v", s)
			}
		}
	}
}

func (p *Poller) Stop(ctx actor.RecvContext) {
	join := make(chan bool)
	p.quit <- join
	<-join
}

func (p *Poller) Recv(m actor.Msg, ctx actor.RecvContext) {}

type SwitchJoined struct {
	Switch Switch
}

type SwitchJoinHandler struct {
	poller *Poller
}

func (s *SwitchJoinHandler) Recv(m actor.Msg, ctx actor.RecvContext) {
	joined := m.Data().(SwitchJoined)
	matrix := ctx.Dict(matrixDict)
	key := joined.Switch.Key()
	_, ok := matrix.Get(key)
	if ok {
		glog.Errorf("Switch already exists in matrix: %+v", joined)
		return
	}
	matrix.Set(key, make(SwitchStats))
	s.poller.query <- StatQuery{joined.Switch}
	glog.Infof("Switch joined: %+v", joined)
}

func (s *SwitchJoinHandler) Map(m actor.Msg, ctx actor.Context) actor.MapSet {
	return actor.MapSet{{matrixDict, m.Data().(SwitchJoined).Switch.Key()}}
}
