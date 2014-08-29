package bh

import (
	"fmt"
	"time"

	"github.com/golang/glog"
)

type statCollector interface {
	collect(from, to BeeId, msg Msg)
}

type dummyStatCollector struct{}

func (c *dummyStatCollector) collect(from, to BeeId, msg Msg) {}
func (c *dummyStatCollector) init(h Hive)                     {}

type appStatCollector struct {
	hive Hive
}

func newAppStatCollector(h Hive) statCollector {
	c := &appStatCollector{hive: h}
	a := h.NewApp("localStatCollector")
	a.Handle(localStatUpdate{}, &localStatCollector{})
	a.Handle(migrateBeeCmdData{}, &localStatCollector{})
	a.Handle(aggrStatUpdate{}, &optimizer{})
	glog.V(1).Infof("App stat collector is registered.")
	return c
}

func (c *appStatCollector) collect(from, to BeeId, msg Msg) {
	switch msg.Data().(type) {
	case localStatUpdate, aggrStatUpdate:
		return
	}

	if from.isNil() || to.isNil() {
		return
	}

	glog.V(3).Infof("Stat collector collects a new message from: %+v --> %+v",
		from, to)
	c.hive.Emit(localStatUpdate{from, to, 1})
}

const (
	localStatDict = "LocalStatistics"
	aggrStatDict  = "AggregatedStatDict"
	aggrHeatDict  = "AggregatedHeapDict"
)

type localStatUpdate struct {
	From  BeeId
	To    BeeId
	Count uint64
}

type communicationStat struct {
	from      BeeId
	to        BeeId
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
	return u.From.HiveId == u.To.HiveId
}

func (u *localStatUpdate) selfCommunication() bool {
	return u.From == u.To
}

func (c *localStatCollector) Map(msg Msg, ctx MapContext) MapSet {
	u := msg.Data().(localStatUpdate)
	return MapSet{{localStatDict, u.Key()}}
}

func (c *localStatCollector) Rcv(msg Msg, ctx RcvContext) {
	switch m := msg.Data().(type) {
	case localStatUpdate:
		d := ctx.Dict(localStatDict)
		k := m.Key()
		v, err := d.Get(k)
		if err != nil {
			v = newCommunicationStat(m)
		}
		s := v.(communicationStat)
		s.add(m.Count)

		if s.countSinceLastEvent() < 10 || s.timeSinceLastEvent() < 1*time.Second {
			d.Put(k, s)
			return
		}

		ctx.Emit(s.toAggrStat())
		d.Put(k, s)

	case migrateBeeCmdData:
		a, ok := ctx.(*rcvContext).hive.app(m.From.AppName)
		if !ok {
			glog.Fatalf("Cannot find app for migrate command: %+v", m)
			return
		}

		resCh := make(chan asyncResult)
		a.qee.ctrlCh <- routineCmd{migrateBeeCmd, m, resCh}
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
	Matrix   map[HiveId]uint64
}

type optimizer struct{}

func (o *optimizer) stat(id BeeId, dict Dictionary) aggrStat {
	v, err := dict.Get(id.Key())
	if err != nil {
		return aggrStat{
			Matrix: make(map[HiveId]uint64),
		}
	}

	return v.(aggrStat)
}

func (o *optimizer) Rcv(msg Msg, ctx RcvContext) {
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

	stat.Matrix[update.From.HiveId] += update.Count
	defer func() {
		dict.Put(update.To.Key(), stat)
	}()

	a, ok := ctx.Hive().(*hive).app(update.To.AppName)
	if !ok {
		glog.Errorf("App not found: %s", update.To.AppName)
		return
	}

	if a.sticky {
		return
	}

	max := uint64(0)
	maxHive := HiveId("")
	for id, cnt := range stat.Matrix {
		if max < cnt {
			max = cnt
			maxHive = id
		}

		if max == cnt && update.To.HiveId == id {
			max = cnt
			maxHive = id
		}
	}

	if maxHive == "" || update.To.HiveId == maxHive {
		return
	}

	glog.Infof("Initiating a migration: %+v", update)
	ctx.SendToBee(migrateBeeCmdData{update.To, maxHive}, msg.From())

	stat.Migrated = true
}

func (o *optimizer) Map(msg Msg, ctx MapContext) MapSet {
	return MapSet{{aggrStatDict, "Centeralized"}}
}
