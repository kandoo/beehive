package beehive

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/golang/glog"
	bhgob "github.com/kandoo/beehive/gob"
	"github.com/kandoo/beehive/state"
)

type collector interface {
	collect(bee uint64, in *msg, out []*msg)
}

type noOpStatCollector struct{}

func (c *noOpStatCollector) collect(bee uint64, in *msg, out []*msg) {}

const (
	appCollector  = "bh_collector"
	dictLocalStat = "LocalStatDict"
	dictLocalProv = "LocalProvDict"
	dictOptimizer = "OptimizerDict"

	defaultMinScore = 3
)

type collectorApp struct {
	hive Hive
}

func newAppStatCollector(h *hive) collector {
	c := &collectorApp{hive: h}
	a := h.NewApp(appCollector, AppNonTransactional)
	a.Handle(beeRecord{}, localCollector{})
	a.Handle(cmdMigrate{}, localCollector{})
	a.Handle(pollLocalStat{}, localStatPoller{
		thresh: uint64(h.config.OptimizeThresh),
	})

	a.Handle(beeMatrixUpdate{}, optimizerCollector{})
	a.Handle(pollOptimizer{}, optimizer{defaultMinScore})

	a.Detached(NewTimer(1*time.Second, func() {
		h.Emit(pollOptimizer{})
		h.Emit(pollLocalStat{})
	}))

	s := NewSync(a)
	s.Handle(statRequest{}, statRequestHandler{})
	a.HTTPHandle("/stats", &statHttpHandler{sync: s})

	glog.V(1).Infof("%v installs app stat collector", h)
	return c
}

type beeRecord struct {
	Bee uint64
	In  *msg
	Out []*msg
}

func formatBeeID(id uint64) string {
	return strconv.FormatUint(id, 10)
}

func parseBeeID(str string) uint64 {
	id, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		glog.Fatalf("error in parsing id: %v", err)
	}
	return id
}

func (c *collectorApp) collect(bee uint64, in *msg, out []*msg) {
	switch in.Data().(type) {
	case beeMatrixUpdate, cmdMigrate:
		return
	}

	if in.From() == Nil {
		return
	}

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

func (c localCollector) Map(msg Msg, ctx MapContext) MappedCells {
	return ctx.LocalMappedCells()
}

func (c localCollector) Rcv(msg Msg, ctx RcvContext) error {
	switch br := msg.Data().(type) {
	case beeRecord:
		c.updateMatrix(br, ctx)
		c.updateProvenance(br, ctx)
	case cmdMigrate:
		bi, err := beeInfoFromContext(ctx, br.Bee)
		if err != nil {
			return fmt.Errorf("%v cannot find bee %v to migrate", ctx, br.Bee)
		}
		a, ok := ctx.(*bee).hive.app(bi.App)
		if !ok {
			return fmt.Errorf("%v cannot find app %v", ctx, a)
		}
		if _, err := a.qee.processCmd(br); err != nil {
			return fmt.Errorf(
				"%v cannot migrate bee %v to %v as instructed by optimizer: %v",
				ctx, br.Bee, br.To, err)
		}
	}
	return nil
}

func (c localCollector) updateMatrix(r beeRecord, ctx RcvContext) {
	d := ctx.Dict(dictLocalStat)
	k := formatBeeID(r.Bee)
	var lm localBeeMatrix
	if err := d.GetGob(k, &lm); err != nil {
		lm.BeeMatrix.Bee = r.Bee
		lm.BeeMatrix.Matrix = make(map[uint64]uint64)
		lm.UpdateTime = time.Now()
	}
	lm.BeeMatrix.Matrix[r.In.From()]++
	lm.UpdateMsgCnt++
	if err := d.PutGob(k, &lm); err != nil {
		glog.Fatalf("cannot store matrix: %v", err)
	}
}

type provMatrix map[string]map[string]uint64

func (c localCollector) updateProvenance(r beeRecord, ctx RcvContext) {
	intype := r.In.Type()
	d := ctx.Dict(dictLocalProv)
	k := formatBeeID(r.Bee)
	var mx provMatrix
	if err := d.GetGob(k, &mx); err != nil {
		mx = make(provMatrix)
	}
	stat, ok := mx[intype]
	if !ok {
		stat = make(map[string]uint64)
		mx[intype] = stat
	}
	for _, msg := range r.Out {
		stat[msg.Type()]++
	}
	if err := d.PutGob(k, &mx); err != nil {
		glog.Fatalf("cannot store provenance data: %v", err)
	}
}

type pollLocalStat struct{}

type localStatPoller struct {
	thresh uint64
}

func (p localStatPoller) Map(msg Msg, ctx MapContext) MappedCells {
	return MappedCells{}
}

func (p localStatPoller) Rcv(msg Msg, ctx RcvContext) error {
	d := ctx.Dict(dictLocalStat)
	d.ForEach(func(k string, v []byte) {
		var lm localBeeMatrix
		if err := bhgob.Decode(&lm, v); err != nil {
			return
		}
		now := time.Now()
		dur := uint64(now.Sub(lm.UpdateTime) / time.Second)
		if dur == 0 {
			dur = 1
		}
		if lm.UpdateMsgCnt/dur < p.thresh {
			return
		}

		ctx.Emit(beeMatrixUpdate(lm.BeeMatrix))
		lm.UpdateTime = now
		lm.UpdateMsgCnt = 0
		d.PutGob(k, &lm)
	})
	return nil
}

// TODO(soheil): implement migration status: none, initiated, and done.
type optimizerStat struct {
	Bee       uint64
	Collector uint64
	Matrix    map[uint64]uint64
	Migrated  bool
	Score     int
	LastMax   uint64
}

type optimizerCollector struct{}

func (c optimizerCollector) isMigrated(b uint64, optDict state.Dict) bool {
	var os optimizerStat
	err := optDict.GetGob(formatBeeID(b), &os)
	return err == nil && os.Migrated
}

func (c optimizerCollector) Rcv(msg Msg, ctx RcvContext) error {
	up := msg.Data().(beeMatrixUpdate)
	glog.V(3).Infof("optimizer receives stat update: %+v", up)
	dict := ctx.Dict(dictOptimizer)
	k := formatBeeID(up.Bee)
	var os optimizerStat
	dict.GetGob(k, &os)
	os.Bee = up.Bee
	os.Collector = msg.From()
	os.Matrix = up.Matrix
	return dict.PutGob(k, &os)
}

var optimizerCentrlizedCells = MappedCells{{dictOptimizer, "0"}}

func (c optimizerCollector) Map(msg Msg, ctx MapContext) MappedCells {
	return optimizerCentrlizedCells
}

type beeHiveCnt struct {
	Bee  uint64
	Hive uint64
	Cnt  uint64
}

type beeHiveStat []beeHiveCnt

func (s beeHiveStat) Len() int           { return len(s) }
func (s beeHiveStat) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s beeHiveStat) Less(i, j int) bool { return s[i].Cnt < s[j].Cnt }

type pollOptimizer struct{}

type optimizer struct {
	minScore int
}

func getOptimizerStats(dict state.Dict) (stats map[uint64]optimizerStat) {
	stats = make(map[uint64]optimizerStat)
	dict.ForEach(func(k string, v []byte) {
		id := parseBeeID(k)
		var os optimizerStat
		if err := bhgob.Decode(&os, v); err != nil {
			glog.Errorf("cannot decode optimizer stat: %v", err)
			return
		}
		stats[id] = os
	})
	return
}

func (o optimizer) Rcv(msg Msg, ctx RcvContext) error {
	dict := ctx.Dict(dictOptimizer)
	stats := getOptimizerStats(dict)

	infos := make(map[uint64]BeeInfo)
	for id, os := range stats {
		infos[id] = BeeInfo{}
		for bid := range os.Matrix {
			infos[bid] = BeeInfo{}
		}
	}

	var err error
	for id := range infos {
		infos[id], err = beeInfoFromContext(ctx, id)
		if err != nil {
			delete(infos, id)
		}
	}

	bhmx := make(map[uint64]map[uint64]uint64)
	for b, os := range stats {
		if os.Migrated {
			continue
		}
		bi, ok := infos[b]
		if !ok || bi.Detached {
			continue
		}
		if app, ok := ctx.Hive().(*hive).app(bi.App); ok && app.sticky() {
			continue
		}
		for fromb, cnt := range os.Matrix {
			if stats[fromb].Migrated {
				continue
			}
			frombi, ok := infos[fromb]
			if !ok {
				continue
			}

			hmx, ok := bhmx[b]
			if !ok {
				hmx = make(map[uint64]uint64)
				bhmx[b] = hmx
			}
			hmx[frombi.Hive] += cnt

			if frombi.Detached {
				continue
			}
			hmx, ok = bhmx[fromb]
			if !ok {
				hmx = make(map[uint64]uint64)
				bhmx[fromb] = hmx
			}
			hmx[bi.Hive] += cnt
		}
	}

	sorted := make(beeHiveStat, 0, len(stats))
	for b, hmx := range bhmx {
		bi := infos[b]
		local := hmx[bi.Hive]
		max := uint64(0)
		maxh := uint64(0)
		for h, cnt := range hmx {
			if h == bi.Hive {
				continue
			}
			if max < cnt {
				max = cnt
				maxh = h
			}
		}
		if max <= 2*local {
			continue
		}
		os := stats[b]
		if max == os.LastMax {
			continue
		}
		os.Score++
		os.LastMax = max
		k := formatBeeID(b)
		dict.PutGob(k, &os)
		if os.Score <= o.minScore {
			continue
		}
		sorted = append(sorted, beeHiveCnt{
			Bee:  b,
			Hive: maxh,
			Cnt:  max,
		})
	}
	if len(sorted) == 0 {
		return nil
	}
	sort.Sort(sorted)

	blacklist := make(map[uint64]struct{})
	for _, bhc := range sorted {
		bi, ok := infos[bhc.Bee]
		if !ok {
			continue
		}
		if _, ok := blacklist[bi.Hive]; ok {
			continue
		}
		blacklist[bhc.Hive] = struct{}{}

		glog.Infof("%v initiates migration of bee %v to hive %v", ctx, bhc.Bee,
			bhc.Hive)
		os := stats[bhc.Bee]
		ctx.SendToBee(cmdMigrate{Bee: bhc.Bee, To: bhc.Hive}, os.Collector)
		os.Migrated = true
		k := formatBeeID(bhc.Bee)
		dict.PutGob(k, &os)
	}
	return nil
}

func (o optimizer) Map(msg Msg, ctx MapContext) MappedCells {
	return optimizerCentrlizedCells
}

type statRequestHandler struct{}

func (h statRequestHandler) Rcv(msg Msg, ctx RcvContext) error {
	dict := ctx.Dict(dictOptimizer)
	res := statResponse{
		Matrix: make(map[uint64]map[uint64]uint64),
	}
	dict.ForEach(func(k string, v []byte) {
		var os optimizerStat
		if err := bhgob.Decode(&os, v); err == nil {
			res.Matrix[os.Bee] = os.Matrix
		}
	})
	return ctx.ReplyTo(msg, res)
}

func (h statRequestHandler) Map(msg Msg, ctx MapContext) MappedCells {
	return optimizerCentrlizedCells
}

func beeInfoFromContext(ctx RcvContext, bid uint64) (BeeInfo, error) {
	return ctx.Hive().(*hive).registry.bee(bid)
}

type statHttpHandler struct {
	sync *Sync
}

func (h *statHttpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	res, err := h.sync.Process(statRequest{})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonres := make(map[string]map[string]uint64)
	for to, m := range res.(statResponse).Matrix {
		jsonresto := make(map[string]uint64)
		jsonres[strconv.FormatUint(to, 10)] = jsonresto
		for from, cnt := range m {
			jsonresto[strconv.FormatUint(from, 10)] = cnt
		}
	}
	b, err := json.Marshal(jsonres)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(b)
}

type statRequest struct{}

type statResponse struct {
	Matrix map[uint64]map[uint64]uint64
}

func init() {
	gob.Register(beeHiveCnt{})
	gob.Register(beeHiveStat{})
	gob.Register(beeMatrix{})
	gob.Register(beeMatrixUpdate{})
	gob.Register(beeRecord{})
	gob.Register(localBeeMatrix{})
	gob.Register(optimizerStat{})
	gob.Register(pollLocalStat{})
	gob.Register(pollOptimizer{})
	gob.Register(provMatrix{})
	gob.Register(statRequest{})
	gob.Register(statResponse{})
}
