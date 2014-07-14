package actor

import (
	"fmt"
	"time"

	"github.com/golang/glog"
)

type statCollector interface {
	collect(from, to RcvrId, msg Msg)
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
	a.Handle(migrateRcvrCmdData{}, &localStatCollector{})
	a.Handle(aggrStatUpdate{}, &optimizer{})
	glog.V(1).Infof("Actor stat collector is registered.")
	return c
}

func (c *actorStatCollector) collect(from, to RcvrId, msg Msg) {
	switch msg.Data().(type) {
	case localStatUpdate, aggrStatUpdate:
		return
	}

	if from.isNil() || to.isNil() {
		return
	}

	//glog.V(2).Infof("Stat collector collects a new message from: %+v --> %+v",
	//from, to)
	c.stage.Emit(localStatUpdate{from, to, 1})
}

const (
	localStatDict = "LocalStatistics"
	aggrStatDict  = "AggregatedStatDict"
	aggrHeatDict  = "AggregatedHeapDict"
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
	return communicationStat{u.From, u.To, 0, 0, time.Time{}}
}

func (s *communicationStat) add(count uint64) {
	s.count += count
}

func (s *communicationStat) toAggrStat() aggrStatUpdate {
	if s.from.isNil() {
		panic(s)
	}
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

func (c *localStatCollector) Recv(msg Msg, ctx RecvContext) {
	switch m := msg.Data().(type) {
	case localStatUpdate:
		d := ctx.Dict(localStatDict)
		k := m.Key()
		v, ok := d.Get(k)
		if !ok {
			v = newCommunicationStat(m)
		}
		s := v.(communicationStat)
		s.add(m.Count)

		if s.countSinceLastEvent() < 10 || s.timeSinceLastEvent() < 1*time.Second {
			d.Set(k, s)
			return
		}

		ctx.Emit(s.toAggrStat())
		d.Set(k, s)

	case migrateRcvrCmdData:
		a, ok := ctx.(*recvContext).stage.actor(m.From.ActorName)
		if !ok {
			glog.Fatalf("Cannot find actor for migrate command: %+v", m)
			return
		}

		resCh := make(chan asyncResult)
		a.mapper.ctrlCh <- routineCmd{migrateRcvrCmd, m, resCh}
		<-resCh
		// TODO(soheil): Maybe handle errors.
	}
}

type aggrStatUpdate localStatUpdate

func (a *aggrStatUpdate) Key() Key {
	return a.To.Key()
}

func (a *aggrStatUpdate) ReverseKey() Key {
	return a.From.Key()
}

type aggrStat struct {
	Migrated bool
	Matrix   map[StageId]uint64
}

type optimizer struct{}

func (o *optimizer) stat(id RcvrId, dict Dictionary) aggrStat {
	v, ok := dict.Get(id.Key())
	if !ok {
		return aggrStat{
			Matrix: make(map[StageId]uint64),
		}
	}

	return v.(aggrStat)
}

func (o *optimizer) Recv(msg Msg, ctx RecvContext) {
	glog.V(3).Infof("Received stat update: %+v", msg.Data())
	update := msg.Data().(aggrStatUpdate)
	if update.To.isDetachedId() {
		return
	}

	dict := ctx.Dict(aggrStatDict)
	stat := o.stat(update.To, dict)
	if stat.Migrated || o.stat(update.From, dict).Migrated {
		return
	}

	stat.Matrix[update.From.StageId] += update.Count
	defer func() {
		dict.Set(update.To.Key(), stat)
	}()

	max := uint64(0)
	maxStage := StageId("")
	for id, cnt := range stat.Matrix {
		if max < cnt {
			max = cnt
			maxStage = id
		}
	}

	if maxStage == "" || update.To.StageId == maxStage {
		return
	}

	glog.Infof("Initiating a migration: %+v", update)
	ctx.SendToRcvr(migrateRcvrCmdData{update.To, maxStage}, msg.From())

	stat.Migrated = true
}

func (o *optimizer) Map(msg Msg, ctx Context) MapSet {
	return MapSet{{aggrStatDict, "Centeralized"}}
}
