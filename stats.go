package beehive

import (
	"encoding/gob"
	"strconv"
	"time"

	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/golang/glog"
	bhgob "github.com/kandoo/beehive/gob"
	"github.com/kandoo/beehive/state"
)

type collector interface {
	collect(bee uint64, in Msg, out []Msg)
}

type dummyStatCollector struct{}

func (c *dummyStatCollector) collect(bee uint64, in Msg, out []Msg) {}

const (
	collectorAppName = "BeehiveStatCollector"
	localStatDict    = "LocalStatDict"
	localProvDict    = "LocalProvDict"
	optimizerDict    = "OptimizerDict"
)

type collectorApp struct {
	hive Hive
}

func newAppStatCollector(h *hive) collector {
	c := &collectorApp{hive: h}
	a := h.NewApp(collectorAppName, AppNonTransactional)
	a.Handle(beeRecord{}, localCollector{})
	a.Handle(cmdMigrate{}, localCollector{})
	a.Handle(beeMatrixUpdate{}, optimizer{})
	a.Handle(pollLocalStat{}, localStatPoller{
		thresh: uint64(h.config.OptimizeThresh),
	})
	a.Detached(Timer{
		Tick: 1 * time.Second,
		Func: func() { h.Emit(pollLocalStat{}) },
	})
	glog.V(1).Infof("%v installs app stat collector", h)
	return c
}

type beeRecord struct {
	Bee uint64
	In  Msg
	Out []Msg
}

func formatBeeID(id uint64) string {
	return strconv.FormatUint(id, 10)
}

func (c *collectorApp) collect(bee uint64, in Msg, out []Msg) {
	switch in.Data().(type) {
	case beeMatrixUpdate, cmdMigrate:
		return
	}

	if in.From() == Nil || in.To() == Nil {
		return
	}

	glog.V(3).Infof("Stat collector collects %v", in)
	// TODO(soheil): We should batch here.
	c.hive.Emit(beeRecord{Bee: bee, In: in, Out: out})
}

type beeMatrix struct {
	Bee    uint64
	Matrix map[uint64]uint64
}

type localBeeMatrix struct {
	BeeMatrix    beeMatrix
	UpdateTime   time.Time
	UpdateMsgCnt uint64
}

type beeMatrixUpdate beeMatrix

type localCollector struct{}

func (h localCollector) Map(msg Msg, ctx MapContext) MappedCells {
	var bid uint64
	switch d := msg.Data().(type) {
	case beeRecord:
		bid = d.Bee
	case cmdMigrate:
		bid = d.Bee
	}
	return MappedCells{{localProvDict, formatBeeID(bid)}}
}

func (h localCollector) Rcv(msg Msg, ctx RcvContext) error {
	switch d := msg.Data().(type) {
	case beeRecord:
		h.updateMatrix(d, ctx)
		h.updateProvenance(d, ctx)
	case cmdMigrate:
		cmd := cmd{
			App:  ctx.App(),
			Data: d,
		}
		q := ctx.(*localBee).qee
		if _, err := q.processCmd(cmd); err != nil {
			glog.Errorf(
				"%v cannot migrate bee %v to %v as instructed by optimizer",
				ctx, d.Bee, d.To)
		}
	}
	return nil
}

func (h localCollector) updateMatrix(r beeRecord, ctx RcvContext) {
	d := ctx.Dict(localStatDict)
	k := formatBeeID(r.Bee)
	var lm localBeeMatrix
	if err := d.GetGob(k, &lm); err != nil {
		lm.BeeMatrix.Bee = r.Bee
		lm.BeeMatrix.Matrix = make(map[uint64]uint64)
	}
	lm.BeeMatrix.Matrix[r.In.From()]++
	lm.UpdateMsgCnt++
	if err := d.PutGob(k, &lm); err != nil {
		glog.Fatalf("cannot store matrix: %v", err)
	}
}

func (h localCollector) updateProvenance(r beeRecord, ctx RcvContext) {
	intype := r.In.Type()
	d := ctx.Dict(localProvDict)
	for _, m := range r.Out {
		var stat map[string]uint64
		if err := d.GetGob(m.Type(), &stat); err != nil {
			stat = make(map[string]uint64)
		}
		stat[intype]++
		if err := d.PutGob(m.Type(), &stat); err != nil {
			glog.Fatalf("cannot store provenance data: %v", err)
		}
	}
}

type pollLocalStat struct{}

type localStatPoller struct {
	thresh uint64
}

func (h localStatPoller) Map(msg Msg, ctx MapContext) MappedCells {
	return MappedCells{}
}

func (h localStatPoller) Rcv(msg Msg, ctx RcvContext) error {
	d := ctx.Dict(localStatDict)
	d.ForEach(func(k string, v []byte) {
		var lm localBeeMatrix
		if err := bhgob.Decode(&lm, v); err != nil {
			return
		}
		now := time.Now()
		dur := uint64(now.Sub(lm.UpdateTime) / time.Second)
		if lm.UpdateMsgCnt/dur < h.thresh {
			return
		}

		ctx.Emit(beeMatrixUpdate(lm.BeeMatrix))
		lm.UpdateTime = now
		lm.UpdateMsgCnt = 0
		d.PutGob(k, &lm)
	})
	return nil
}

type optimizerStat struct {
	BeeMatrix beeMatrix
	Migrated  bool
}

type optimizer struct{}

func (o optimizer) isMigrated(b uint64, optDict state.Dict) bool {
	_, err := optDict.Get(formatBeeID(b))
	return err == nil
}

func (o optimizer) Rcv(msg Msg, ctx RcvContext) error {
	up := msg.Data().(beeMatrixUpdate)
	glog.V(3).Infof("optimizer receives stat update: %+v", up)
	bi, err := beeInfoFromContext(ctx, up.Bee)
	if err != nil {
		return err
	}

	if bi.Detached {
		return nil
	}

	dict := ctx.Dict(optimizerDict)
	if o.isMigrated(up.Bee, dict) {
		return nil
	}

	var maxCnt uint64
	maxBee := bi
	for from, cnt := range up.Matrix {
		frombi, err := beeInfoFromContext(ctx, from)
		if err != nil {
			continue
		}

		if maxCnt < cnt {
			maxCnt = cnt
			maxBee = frombi
		} else if maxCnt == cnt && bi.Hive == frombi.Hive {
			maxBee = frombi
		}
	}

	if maxBee.Hive == bi.Hive {
		return nil
	}

	// Migrated.
	if o.isMigrated(maxBee.ID, dict) {
		return nil
	}

	glog.Infof("%v initiates migration of %v to %v", ctx, up.Bee, maxBee.Hive)
	ctx.SendToBee(cmdMigrate{up.Bee, maxBee.Hive}, msg.From())
	dict.Put(formatBeeID(up.Bee), []byte{})
	return nil
}

func (o optimizer) Map(msg Msg, ctx MapContext) MappedCells {
	return MappedCells{{optimizerDict, "0"}}
}

func beeInfoFromContext(ctx RcvContext, bid uint64) (BeeInfo, error) {
	return ctx.Hive().(*hive).registry.bee(bid)
}

func init() {
	gob.Register(beeMatrixUpdate{})
}
