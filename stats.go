package beehive

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"strconv"
	"time"

	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/golang/glog"
	"github.com/kandoo/beehive/state"
)

type statCollector interface {
	collect(from, to uint64, msg Msg, emitted []Msg)
}

type dummyStatCollector struct{}

func (c *dummyStatCollector) collect(from, to uint64, msg Msg, emitted []Msg) {}

const (
	appStatCollectorName = "BeehiveStatCollector"
	localStatDict        = "LocalStatDict"
	aggrStatDict         = "AggrStatDict"
	aggrHeatDict         = "AggrHeatDict"
)

type appStatCollector struct {
	hive Hive
}

func newAppStatCollector(h Hive) statCollector {
	c := &appStatCollector{hive: h}
	a := h.NewApp(appStatCollectorName)
	a.Handle(localStatUpdate{}, &localStatCollector{})
	a.Handle(cmdMigrate{}, &localStatCollector{})
	a.Handle(aggrStatUpdate{}, &optimizer{})
	glog.V(1).Infof("%v installs app stat collector", h)
	return c
}

func (c *appStatCollector) collect(from, to uint64, msg Msg, emitted []Msg) {
	switch msg.Data().(type) {
	case localStatUpdate, aggrStatUpdate:
		return
	}

	if from == Nil || to == Nil {
		return
	}

	glog.V(3).Infof("Stat collector collects new %v message from %v --> %v",
		msg.Type(), from, to)
	// TODO(soheil): We should batch here.
	c.hive.Emit(localStatUpdate{from, to, 1})
}

type localStatUpdate struct {
	From  uint64
	To    uint64
	Count uint64
}

type communicationStat struct {
	From      uint64
	To        uint64
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
	if s.From == Nil {
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

func (u *localStatUpdate) key() string {
	return fmt.Sprintf("%#v-%#v", u.From, u.To)
}

func (u *localStatUpdate) selfCommunication() bool {
	return u.From == u.To
}

func (c *localStatCollector) Map(msg Msg, ctx MapContext) MappedCells {
	u := msg.Data().(localStatUpdate)
	return MappedCells{{localStatDict, u.key()}}
}

func beeInfoFromContext(ctx RcvContext, bid uint64) (BeeInfo, error) {
	return ctx.Hive().(*hive).registry.bee(bid)
}

func (c *localStatCollector) Rcv(msg Msg, ctx RcvContext) error {
	switch m := msg.Data().(type) {
	case localStatUpdate:
		d := ctx.Dict(localStatDict)
		k := m.key()
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
			d.Put(k, s.Bytes())
			return nil
		}

		ctx.Emit(s.toAggrStat())
		d.Put(k, s.Bytes())

	case cmdMigrate:
		cmd := cmd{
			App:  ctx.App(),
			Data: m,
		}
		q := ctx.(*localBee).qee
		_, err := q.processCmd(cmd)
		if err != nil {
			glog.Errorf(
				"%v cannot migrate bee %v to %v as instructed by the optimizer", c,
				m.Bee, m.To)
		}
		return err
		// TODO(soheil): Maybe handle errors.
	}

	return nil
}

type aggrStatUpdate localStatUpdate

func (a *aggrStatUpdate) key() string {
	return strconv.FormatUint(a.To, 10)
}

func (a *aggrStatUpdate) reverseKey() string {
	return strconv.FormatUint(a.From, 10)
}

type aggrStat struct {
	Migrated bool
	Matrix   map[uint64]uint64
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

func (o *optimizer) stat(to string, dict state.Dict) aggrStat {
	v, err := dict.Get(to)
	if err != nil {
		return aggrStat{
			Matrix: make(map[uint64]uint64),
		}
	}

	return aggrStatFromBytes([]byte(v))
}

type HiveIDSlice []uint64

func (s HiveIDSlice) Len() int           { return len(s) }
func (s HiveIDSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s HiveIDSlice) Less(i, j int) bool { return s[i] < s[j] }

func (o *optimizer) Rcv(msg Msg, ctx RcvContext) error {
	glog.V(3).Infof("Received stat update: %+v", msg.Data())
	update := msg.Data().(aggrStatUpdate)
	tob, err := beeInfoFromContext(ctx, update.To)
	if err != nil {
		return err
	}

	if tob.Detached {
		return nil
	}

	dict := ctx.Dict(aggrStatDict)
	stat := o.stat(update.key(), dict)
	if stat.Migrated || o.stat(update.reverseKey(), dict).Migrated {
		return nil
	}

	fromb, err := beeInfoFromContext(ctx, update.From)
	if err != nil {
		return err
	}

	stat.Matrix[fromb.Hive] += update.Count
	defer dict.Put(update.key(), stat.Bytes())

	a, ok := ctx.Hive().(*hive).app(tob.App)
	if !ok {
		return fmt.Errorf("%v does not have app %s", ctx.Hive(), tob.App)
	}

	if a.sticky() {
		return nil
	}

	var max uint64
	maxHive := Nil
	for id, cnt := range stat.Matrix {
		if max < cnt {
			max = cnt
			maxHive = id
		}

		if max == cnt && tob.Hive == id {
			max = cnt
			maxHive = id
		}
	}

	if maxHive == Nil || maxHive == tob.Hive {
		return nil
	}

	glog.Infof("%v initiates migration of %v to %v", ctx, update.To, maxHive)
	ctx.SendToBee(cmdMigrate{update.To, maxHive}, msg.From())

	stat.Migrated = true
	return nil
}

func (o *optimizer) Map(msg Msg, ctx MapContext) MappedCells {
	return MappedCells{{aggrStatDict, "Centeralized"}}
}
