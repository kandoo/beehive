package actor

import (
	"fmt"
	"time"

	"github.com/golang/glog"
)

type statCollector interface {
	collect(from, to RcvrId, msg Msg)
}

func (s *stage) init() {
	if s.config.Instrument {
		s.collector = newActorStatCollector(s)
	} else {
		s.collector = &dummyStatCollector{}
	}
}

type dummyStatCollector struct{}

func (c *dummyStatCollector) collect(from, to RcvrId, msg Msg) {}
func (c *dummyStatCollector) init(s Stage)                     {}

type actorStatCollector struct {
	stage Stage
}

func newActorStatCollector(s Stage) statCollector {
	c := &actorStatCollector{stage: s}
	a := s.NewActor("localStatCollector")
	a.Handle(localStatUpdate{}, &localStatCollector{})
	a.Handle(aggrStatUpdate{}, &optimizer{})
	glog.V(1).Infof("Actor stat collector is registered.")
	return c
}

func (c *actorStatCollector) collect(from, to RcvrId, msg Msg) {
	switch msg.Data().(type) {
	case localStatUpdate, aggrStatUpdate:
		return
	}

	glog.V(2).Infof("Stat collector collects a new message from: %+v --> %+v",
		from, to)
	c.stage.Emit(localStatUpdate{from, to, 1})
}

const (
	localStatDict = "LocalStatistics"
	aggrStatDict  = "AggregatedStatDict"
)

type localStatUpdate struct {
	From  RcvrId
	To    RcvrId
	Count uint64
}

type communicationStat struct {
	from      RcvrId
	to        RcvrId
	count     uint64
	lastCount uint64
	lastEvent time.Time
}

func newCommunicationStat(u localStatUpdate) communicationStat {
	return communicationStat{u.From, u.To, u.Count, 0, time.Time{}}
}

func (s *communicationStat) add(count uint64) {
	s.count += count
}

func (s *communicationStat) toAggrStat() aggrStatUpdate {
	u := aggrStatUpdate{s.from, s.to, s.count}
	s.lastEvent = time.Now()
	s.lastCount = s.count
	return u
}

func (s *communicationStat) countSinceLastEvent() uint64 {
	return s.count - s.lastCount
}

func (s *communicationStat) timeSinceLastEvent() time.Duration {
	return time.Now().Sub(s.lastEvent)
}

type localStatCollector struct{}

func (u *localStatUpdate) Key() Key {
	return Key(fmt.Sprintf("%#v-%#v", u.From, u.To))
}

func (u *localStatUpdate) localCommunication() bool {
	return u.From.StageId == u.To.StageId
}

func (u *localStatUpdate) selfCommunication() bool {
	return u.From == u.To
}

func (c *localStatCollector) Map(msg Msg, ctx Context) MapSet {
	u := msg.Data().(localStatUpdate)
	return MapSet{{localStatDict, u.Key()}}
}

func updateStat(u localStatUpdate, d Dictionary) communicationStat {
	k := u.Key()
	v, ok := d.Get(k)
	if !ok {
		s := newCommunicationStat(u)
		d.Set(k, s)
		return s
	}

	s := v.(communicationStat)
	s.add(u.Count)
	d.Set(k, s)
	return s
}

func (c *localStatCollector) Recv(msg Msg, ctx RecvContext) {
	switch d := msg.Data().(type) {
	case localStatUpdate:
		s := updateStat(d, ctx.Dict(localStatDict))

		if s.countSinceLastEvent() < 1 || s.timeSinceLastEvent() < 1*time.Second {
			return
		}

		ctx.Emit(s.toAggrStat())
	case migrateRcvrCmdData:
		a, ok := ctx.(*recvContext).stage.actor(d.From.ActorName)
		if !ok {
			glog.Fatalf("Cannot find actor: %+v", d.From.ActorName)
			return
		}

		resCh := make(chan asyncResult)
		a.mapper.ctrlCh <- routineCmd{migrateRcvrCmd, d, resCh}
		<-resCh
		// TODO(soheil): Maybe handle errors.
	}
}

type aggrStatUpdate localStatUpdate

type optimizer struct{}

func (o *optimizer) Recv(msg Msg, ctx RecvContext) {
	glog.V(1).Infof("Received stat update: %+v", msg.Data())
	updateStat(localStatUpdate(msg.Data().(aggrStatUpdate)),
		ctx.Dict(aggrStatDict))
}

func (o *optimizer) Map(msg Msg, ctx Context) MapSet {
	return MapSet{{aggrStatDict, "Centeralized"}}
}
