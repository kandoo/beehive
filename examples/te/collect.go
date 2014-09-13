package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"strconv"
	"time"

	"github.com/golang/glog"
	"github.com/soheilhy/beehive/bh"
)

type IPV4 uint32
type Switch uint64

func (s Switch) String() string {
	return strconv.FormatUint(uint64(s), 10)
}

func (s Switch) Key() bh.Key {
	return bh.Key(s.String())
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

func (s *SwitchStats) decode(b []byte) {
	dec := gob.NewDecoder(bytes.NewBuffer(b))
	err := dec.Decode(s)
	if err != nil {
		glog.Fatalf("Cannot decode buffer %v", err)
	}
}

func (s *SwitchStats) encode() []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(s)
	if err != nil {
		glog.Fatalf("Error in encoding the stats %v", err)
	}
	return buf.Bytes()
}

const (
	matrixDict = "Matrix"
)

type Collector struct {
	delta  uint64
	poller *Poller
}

func (c *Collector) Rcv(m bh.Msg, ctx bh.RcvContext) error {
	res := m.Data().(StatResult)
	glog.V(2).Infof("Stat results: %+v", res)
	matrix := ctx.Dict(matrixDict)
	key := res.Switch.Key()
	v, err := matrix.Get(key)
	if err != nil {
		return fmt.Errorf("No such switch in matrix: %+v", res)
	}

	c.poller.query <- StatQuery{res.Switch}

	var sw SwitchStats
	sw.decode(v)
	stat, ok := sw[res.Flow]
	sw[res.Flow] = res.Bytes

	glog.V(2).Infof("Previous stats: %+v, Now: %+v", stat, res.Bytes)
	if !ok || res.Bytes-stat > c.delta {
		glog.Infof("Found an elephent flow: %+v, %+v, %+v", res, stat,
			ctx.Hive().ID())
		ctx.Emit(MatrixUpdate(res))
	}

	matrix.Put(key, sw.encode())
	return nil
}

func (c *Collector) Map(m bh.Msg, ctx bh.MapContext) bh.MappedCells {
	return bh.MappedCells{{matrixDict, m.Data().(StatResult).Switch.Key()}}
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

func (p *Poller) Start(ctx bh.RcvContext) {
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
				glog.V(2).Infof("Queried switch: %+v", s)
			}
		}
	}
}

func (p *Poller) Stop(ctx bh.RcvContext) {
	join := make(chan bool)
	p.quit <- join
	<-join
}

func (p *Poller) Rcv(m bh.Msg, ctx bh.RcvContext) error {
	return nil
}

type SwitchJoined struct {
	Switch Switch
}

type SwitchJoinHandler struct {
	poller *Poller
}

func (s *SwitchJoinHandler) Rcv(m bh.Msg, ctx bh.RcvContext) error {
	if m.From().AppName == "" {
		return nil
	}

	joined := m.Data().(SwitchJoined)
	matrix := ctx.Dict(matrixDict)
	key := joined.Switch.Key()
	_, err := matrix.Get(key)
	if err != nil {
		return fmt.Errorf("Switch already exists in matrix: %+v", joined)
	}
	sw := make(SwitchStats)
	matrix.Put(key, sw.encode())

	s.poller.query <- StatQuery{joined.Switch}
	glog.Infof("Switch joined: %+v", joined)
	return nil
}

func (s *SwitchJoinHandler) Map(m bh.Msg,
	ctx bh.MapContext) bh.MappedCells {

	return bh.MappedCells{{matrixDict, m.Data().(SwitchJoined).Switch.Key()}}
}
