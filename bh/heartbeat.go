package bh

import (
	"fmt"
	"time"

	"github.com/golang/glog"
)

const (
	heartbeatAppName = "Heartbeat"
)

// Heartbeat is the based structor for both request and response.
// If the BeeID.ID is empty, it heartbeat the application. It's an empty ID it
// heartbeats the server.
type heartbeat struct {
	TimeStamp time.Time
	BeeID     BeeID
}

type heartbeatReq heartbeat
type heartbeatRes heartbeat

type startHeartbeat BeeID
type stopHeartbeat BeeID

type beeFailed struct {
	id BeeID
}

type stopHeartbeating struct {
	resCh chan interface{}
}

type pulseTaker struct {
	hive          *hive
	ctrlCh        chan interface{}
	dataCh        chan Msg
	hbMap         map[BeeID]heartbeat
	queryInterval time.Duration
	deadInterval  time.Duration
}

func startHeartbeatHandler(h *hive) *pulseTaker {
	h.RegisterMsg(heartbeatRes{})
	h.RegisterMsg(heartbeatReq{})

	hb := pulseTaker{
		hive:          h,
		ctrlCh:        make(chan interface{}),
		dataCh:        make(chan Msg),
		hbMap:         make(map[BeeID]heartbeat),
		queryInterval: h.config.HBQueryInterval,
		deadInterval:  h.config.HBDeadTimeout,
	}

	a := h.NewApp(heartbeatAppName)
	a.Detached(&hb)
	return &hb
}

func (p *pulseTaker) Start(ctx RcvContext) {
	glog.V(2).Infof("Heartbeat daemon is started on %v", ctx.Hive().ID())
	for {
		select {
		case c := <-p.ctrlCh:
			switch cmd := c.(type) {
			case stopHeartbeating:
				cmd.resCh <- true
				glog.V(2).Infof("Heartbeat daemon is stopped on %v", ctx.Hive().ID())
				return
			}
		case m := <-p.dataCh:
			p.handleHeartbeat(m, ctx)
		case <-time.After(p.queryInterval):
			p.heartbeatAll(ctx)
		}
	}
}

func (p *pulseTaker) handleHeartbeat(msg Msg, ctx RcvContext) {
	// TODO(soheil): No need for channels here. Change to mutex.
	switch d := msg.Data().(type) {
	case heartbeatReq:
		ctx.ReplyTo(msg, heartbeatRes(d))
	case heartbeatRes:
		hb, ok := p.hbMap[d.BeeID]
		if !ok {
			glog.Errorf("Received heartbeat response that we have not requested: %v",
				d.BeeID)
			return
		}
		// Reset the timestamp to make sure there is no spurious timeout when we
		// have not sent a request yet.
		hb.TimeStamp = time.Time{}
		p.hbMap[d.BeeID] = hb
		glog.V(2).Infof("Heartbeat received from %v on %v", hb.BeeID,
			ctx.Hive().ID())
	case startHeartbeat:
		glog.V(2).Infof("Starting heartbeat for %v on %v", BeeID(d),
			ctx.Hive().ID())
		p.hbMap[BeeID(d)] = heartbeat{
			BeeID: BeeID(d),
		}
	}
}

func (p *pulseTaker) isToHive(b BeeID) bool {
	return b.ID == 0 && len(b.AppName) == 0
}

func (p *pulseTaker) heartbeatAll(ctx RcvContext) {
	for b, hb := range p.hbMap {
		if hb.TimeStamp.Equal(time.Time{}) {
			hb.TimeStamp = time.Now()
			p.hbMap[b] = hb
		} else if time.Since(hb.TimeStamp) > p.deadInterval {
			if id := hb.BeeID; p.isToHive(id) {
				glog.Errorf("%v announces hive %v dead", ctx.Hive().ID(), id.HiveID)
				ctx.Emit(HiveLeft{HiveID: id.HiveID})
			} else {
				glog.Errorf("%v announces bee %v dead", ctx.Hive().ID(), hb.BeeID)
				ctx.Emit(beeFailed{id: hb.BeeID})
			}
			delete(p.hbMap, b)
			continue
		}

		hb.TimeStamp = time.Now()
		// TODO(soheil): There might be a problem here. What if SendToBee blocks
		// because of an overwhelmed channel. Shouldn't we update "now"?
		if hb.BeeID.ID == 0 && len(hb.BeeID.AppName) == 0 {
			id := hb.BeeID.HiveID
			ctx.SendToBee(heartbeatReq(hb), pulseTakerID(id))
			glog.V(2).Infof("Heartbeat sent to hive %v", id)
		} else {
			id := hb.BeeID
			ctx.SendToBee(heartbeatReq(hb), id)
			glog.V(2).Infof("Heartbeat sent to bee %v", id)
		}
	}
}

func (p *pulseTaker) Stop(ctx RcvContext) {
	ch := make(chan interface{})
	p.ctrlCh <- stopHeartbeating{ch}
	<-ch
}

func (p *pulseTaker) Rcv(m Msg, ctx RcvContext) error {
	p.dataCh <- m
	return nil
}

func pulseTakerID(h HiveID) BeeID {
	return BeeID{
		HiveID:   h,
		AppName:  heartbeatAppName,
		ID:       1, // We only have one bee and the ID of that bee would be one.
		Detached: true,
	}
}

func startHeartbeatBee(b BeeID, h *hive) {
	if b.AppName == heartbeatAppName {
		return
	}
	if h.config.UseBeeHeartbeat {
		h.SendToBee(startHeartbeat(b), pulseTakerID(h.ID()))
	}
	b.AppName = ""
	b.ID = 0
	h.SendToBee(startHeartbeat(b), pulseTakerID(h.ID()))
}

func stopHeartbeatBee(b BeeID, h Hive) {
	h.SendToBee(stopHeartbeat(b), pulseTakerID(h.ID()))
}

type heartbeatReqHandler struct{}

func (h *heartbeatReqHandler) Rcv(msg Msg, ctx RcvContext) error {
	ctx.AbortTx()

	if hb, ok := msg.Data().(heartbeatReq); ok {
		ctx.ReplyTo(msg, heartbeatRes(hb))
		return nil
	}

	return fmt.Errorf("An invalid message received: %+v", msg)
}

func (h *heartbeatReqHandler) Map(msg Msg, ctx MapContext) MappedCells {
	glog.Fatal("Heartbeat requests should always be unicast.")
	return nil
}
