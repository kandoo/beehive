package beehive

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
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
	appCollector  = "bh_collector"
	dictLocalStat = "LocalStatDict"
	dictLocalProv = "LocalProvDict"
	dictOptimizer = "OptimizerDict"
)

type collectorApp struct {
	hive Hive
}

func newAppStatCollector(h *hive) collector {
	c := &collectorApp{hive: h}
	a := h.NewApp(appCollector, AppNonTransactional)
	a.Handle(beeRecord{}, localCollector{})
	a.Handle(cmdMigrate{}, localCollector{})
	a.Handle(beeMatrixUpdate{}, optimizer{})
	a.Handle(pollLocalStat{}, localStatPoller{
		thresh: uint64(h.config.OptimizeThresh),
	})
	a.Handle(statRequest{}, statRequestHandler{})
	a.Detached(NewTimer(1*time.Second, func() { h.Emit(pollLocalStat{}) }))
	hh := newStatHttpHandler()
	a.Detached(hh)
	a.HTTPHandle("/stats", hh)
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
		fmt.Println(dur, lm.UpdateMsgCnt, p.thresh)
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

type optimizerStat struct {
	BeeMatrix beeMatrix
	Migrated  bool
}

type optimizer struct{}

func (o optimizer) isMigrated(b uint64, optDict state.Dict) bool {
	var os optimizerStat
	err := optDict.GetGob(formatBeeID(b), &os)
	return err == nil && os.Migrated
}

func (o optimizer) Rcv(msg Msg, ctx RcvContext) error {
	up := msg.Data().(beeMatrixUpdate)
	glog.V(3).Infof("optimizer receives stat update: %+v", up)
	bi, err := beeInfoFromContext(ctx, up.Bee)
	if err != nil {
		return err
	}

	os := optimizerStat{
		BeeMatrix: beeMatrix(up),
		Migrated:  false,
	}
	dict := ctx.Dict(dictOptimizer)
	defer func() {
		dict.PutGob(formatBeeID(up.Bee), &os)
	}()

	if bi.Detached {
		return nil
	}

	if o.isMigrated(up.Bee, dict) {
		os.Migrated = true
		return nil
	}

	var maxCnt uint64
	maxBee := bi
	for from, cnt := range up.Matrix {
		frombi, err := beeInfoFromContext(ctx, from)
		if err != nil || o.isMigrated(frombi.ID, dict) {
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

	glog.Infof("%v initiates migration of bee %v to hive %v", ctx, up.Bee,
		maxBee.Hive)
	ctx.SendToBee(cmdMigrate{Bee: up.Bee, To: maxBee.Hive}, msg.From())
	os.Migrated = true
	return nil
}

var optimizerCentrlizedCells = MappedCells{{dictOptimizer, "0"}}

func (o optimizer) Map(msg Msg, ctx MapContext) MappedCells {
	return optimizerCentrlizedCells
}

type statRequestHandler struct{}

func (h statRequestHandler) Rcv(msg Msg, ctx RcvContext) error {
	req := msg.Data().(statRequest)
	dict := ctx.Dict(dictOptimizer)
	res := statResponse{
		ID:     req.ID,
		Matrix: make(map[uint64]map[uint64]uint64),
	}
	dict.ForEach(func(k string, v []byte) {
		var os optimizerStat
		if err := bhgob.Decode(&os, v); err == nil {
			res.Matrix[os.BeeMatrix.Bee] = os.BeeMatrix.Matrix
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
	reqch   chan statRequestAndChannel
	resch   chan statResponse
	done    chan struct{}
	pending map[uint64]statRequestAndChannel
}

func (h *statHttpHandler) Start(ctx RcvContext) {
	for {
		select {
		case rc := <-h.reqch:
			h.pending[rc.r.ID] = rc
			ctx.Emit(rc.r)

		case res := <-h.resch:
			rc, ok := h.pending[res.ID]
			if !ok {
				glog.Errorf("cannot find request %v", res.ID)
				continue
			}
			rc.c <- res

		case <-h.done:
			return
		}
	}
}

func (h *statHttpHandler) Stop(ctx RcvContext) {
	h.done <- struct{}{}
}

func (h *statHttpHandler) Rcv(msg Msg, ctx RcvContext) error {
	h.resch <- msg.Data().(statResponse)
	return nil
}

func (h statHttpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ch := make(chan statResponse)
	h.reqch <- statRequestAndChannel{
		r: statRequest{ID: uint64(rand.Int63())},
		c: ch,
	}
	res := <-ch
	jsonres := make(map[string]map[string]uint64)
	for to, m := range res.Matrix {
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

func newStatHttpHandler() *statHttpHandler {
	return &statHttpHandler{
		reqch:   make(chan statRequestAndChannel),
		resch:   make(chan statResponse),
		done:    make(chan struct{}),
		pending: make(map[uint64]statRequestAndChannel),
	}
}

type statRequest struct {
	ID uint64
}

type statResponse struct {
	ID     uint64
	Matrix map[uint64]map[uint64]uint64
}

type statRequestAndChannel struct {
	r statRequest
	c chan<- statResponse
}

func init() {
	gob.Register(beeMatrix{})
	gob.Register(beeMatrixUpdate{})
	gob.Register(provMatrix{})
	gob.Register(statRequest{})
	gob.Register(statResponse{})
}
