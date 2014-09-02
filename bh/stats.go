package bh

import (
	"bytes"
	"encoding/gob"
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
	a := h.NewApp("StatCollector")
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
	From      BeeId
	To        BeeId
	Count     uint64
	LastCount uint64
	LastEvent time.Time
}

func newCommunicationStat(u localStatUpdate) communicationStat {
	return communicationStat{u.From, u.To, 0, 0, time.Time{}}
}

func communicationStatFromBytes(b []byte) communicationStat {
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	var c communicationStat
	// We ignore the error here.
	dec.Decode(&c)
	return c
}

func (c *communicationStat) Bytes() []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(&c)
	if err != nil {
		return nil
	}
	return buf.Bytes()
}

func (s *communicationStat) add(count uint64) {
	s.Count += count
}

func (s *communicationStat) toAggrStat() aggrStatUpdate {
	if s.From.isNil() {
		panic(s)
	}
	u := aggrStatUpdate{s.From, s.To, s.Count}
	s.LastEvent = time.Now()
	s.LastCount = s.Count
	return u
}

func (s *communicationStat) countSinceLastEvent() uint64 {
	return s.Count - s.LastCount
}

func (s *communicationStat) timeSinceLastEvent() time.Duration {
	return time.Now().Sub(s.LastEvent)
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

func (c *localStatCollector) Rcv(msg Msg, ctx RcvContext) error {
	switch m := msg.Data().(type) {
	case localStatUpdate:
		d := ctx.Dict(localStatDict)
		k := m.Key()
		v, err := d.Get(k)
		var s communicationStat
		if err == nil {
			// We can ignore the error here.
			s = communicationStatFromBytes([]byte(v))
		} else {
			s = newCommunicationStat(m)
		}
		s.add(m.Count)

		if s.countSinceLastEvent() < 10 || s.timeSinceLastEvent() < 1*time.Second {
			d.Put(k, Value(s.Bytes()))
			return nil
		}

		ctx.Emit(s.toAggrStat())
		d.Put(k, Value(s.Bytes()))

	case migrateBeeCmdData:
		a, ok := ctx.(*rcvContext).hive.app(m.From.AppName)
		if !ok {
			return fmt.Errorf("Cannot find app for migrate command: %+v", m)
		}

		resCh := make(chan asyncResult)
		a.qee.ctrlCh <- routineCmd{migrateBeeCmd, m, resCh}
		<-resCh
		// TODO(soheil): Maybe handle errors.
	}

	return nil
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

func aggrStatFromBytes(b []byte) aggrStat {
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	var s aggrStat
	dec.Decode(&s)
	return s
}

func (s *aggrStat) Bytes() []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	enc.Encode(s)
	return buf.Bytes()
}

type optimizer struct{}

func (o *optimizer) stat(id BeeId, dict Dictionary) aggrStat {
	v, err := dict.Get(id.Key())
	if err != nil {
		return aggrStat{
			Matrix: make(map[HiveId]uint64),
		}
	}

	return aggrStatFromBytes([]byte(v))
}

type HiveIdSlice []HiveId

func (s HiveIdSlice) Len() int           { return len(s) }
func (s HiveIdSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s HiveIdSlice) Less(i, j int) bool { return s[i] < s[j] }

func (o *optimizer) Rcv(msg Msg, ctx RcvContext) error {
	glog.V(3).Infof("Received stat update: %+v", msg.Data())
	update := msg.Data().(aggrStatUpdate)
	if update.To.isDetachedId() {
		return nil
	}

	dict := ctx.Dict(aggrStatDict)
	stat := o.stat(update.To, dict)
	if stat.Migrated || o.stat(update.From, dict).Migrated {
		return nil
	}

	stat.Matrix[update.From.HiveId] += update.Count
	defer func() {
		dict.Put(update.To.Key(), stat.Bytes())
	}()

	a, ok := ctx.Hive().(*hive).app(update.To.AppName)
	if !ok {
		return fmt.Errorf("App not found: %s", update.To.AppName)
	}

	if a.sticky {
		return nil
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
		return nil
	}

	glog.Infof("Initiating a migration: %+v", update)
	ctx.SendToBee(migrateBeeCmdData{update.To, maxHive}, msg.From())

	stat.Migrated = true
	return nil
}

func (o *optimizer) Map(msg Msg, ctx MapContext) MapSet {
	return MapSet{{aggrStatDict, "Centeralized"}}
}
