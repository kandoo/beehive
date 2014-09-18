package bh

import (
	"fmt"
	"time"

	"github.com/golang/glog"
)

const (
	heartbeatAppName = "Heartbeat"
)

type heartbeat struct {
	TimeStamp int64
	// If the BeeID.ID is empty, it heartbeat the application. It's an empty ID it
	// heartbeats the server.
	BeeID BeeID
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
	for {
		select {
		case c := <-p.ctrlCh:
			switch cmd := c.(type) {
			case stopHeartbeating:
				close(p.dataCh)
				close(p.ctrlCh)
				cmd.resCh <- true
				glog.V(2).Infof("Heartbeat stopped")
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
	switch d := msg.Data().(type) {
	case heartbeatReq:
		ctx.ReplyTo(msg, heartbeatRes(d))
	case heartbeatRes:
		hb, ok := p.hbMap[d.BeeID]
		if !ok {
			glog.Errorf("Received heartbeat response that we have not requested: %+v",
				d.BeeID)
		}
		hb.TimeStamp = time.Now().Unix()
		glog.V(2).Infof("Heartbeat received from %+v", hb.BeeID)
	case startHeartbeat:
		p.hbMap[BeeID(d)] = heartbeat{BeeID: BeeID(d)}
	}
}

func (p *pulseTaker) heartbeatAll(ctx RcvContext) {
	now := time.Now().Unix()
	for _, hb := range p.hbMap {
		if now-hb.TimeStamp > int64(p.deadInterval*time.Millisecond) {
			glog.Infof("A bee is found dead: %+v", hb.BeeID)
			ctx.Emit(beeFailed{id: hb.BeeID})
			continue
		}

		hb.TimeStamp = time.Now().Unix()
		// TODO(soheil): There might be a problem here. What if SendToBee blocks
		// because of an overwhelmed channel. Shouldn't we update "now"?
		ctx.SendToBee(heartbeatReq(hb), hb.BeeID)
		glog.V(2).Infof("Heartbeat sent to %+v", hb.BeeID)
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

func startHeartbeatBee(b BeeID, h Hive) {
	if b.AppName == heartbeatAppName {
		return
	}

	h.SendToBee(startHeartbeat(b), pulseTakerID(h.ID()))
}

func stopHeartbeatBee(b BeeID, h Hive) {
	h.SendToBee(stopHeartbeat(b), pulseTakerID(h.ID()))
}

type heartbeatReqHandler struct{}

func (h *heartbeatReqHandler) Rcv(msg Msg, ctx RcvContext) error {
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
