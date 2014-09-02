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
	// If the BeeId.Id is empty, it heartbeat the application. It's an empty ID it
	// heartbeats the server.
	BeeId BeeId
}

type heartbeatReq heartbeat
type heartbeatRes heartbeat

type startHeartbeat BeeId
type stopHeartbeat BeeId

type beeFailed struct {
	id BeeId
}

type stopHeartbeating struct {
	resCh chan interface{}
}

type pulseTaker struct {
	ctrlCh        chan interface{}
	dataCh        chan Msg
	hbMap         map[BeeId]heartbeat
	queryInterval time.Duration
	deadInterval  time.Duration
}

func startHeartbeatHandler(h *hive) *pulseTaker {
	h.RegisterMsg(heartbeatRes{})
	h.RegisterMsg(heartbeatReq{})

	hb := pulseTaker{
		ctrlCh:        make(chan interface{}),
		dataCh:        make(chan Msg),
		hbMap:         make(map[BeeId]heartbeat),
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
		hb, ok := p.hbMap[d.BeeId]
		if !ok {
			glog.Errorf("Received heartbeat response that we have not requested: %+v",
				d.BeeId)
		}
		hb.TimeStamp = time.Now().Unix()
		glog.Infof("Heartbeat received from %+v", hb.BeeId)
	case startHeartbeat:
		p.hbMap[BeeId(d)] = heartbeat{BeeId: BeeId(d)}
	}
}

func (p *pulseTaker) heartbeatAll(ctx RcvContext) {
	now := time.Now().Unix()
	for _, hb := range p.hbMap {
		if now-hb.TimeStamp > int64(p.deadInterval*time.Millisecond) {
			glog.Infof("A bee is found dead: %+v", hb.BeeId)
			ctx.Emit(beeFailed{id: hb.BeeId})
			continue
		}

		hb.TimeStamp = time.Now().Unix()
		// TODO(soheil): There might be a problem here. What if SendToBee blocks
		// because of an overwhelmed channel. Shouldn't we update "now"?
		ctx.SendToBee(heartbeatReq(hb), hb.BeeId)
		glog.Infof("Heartbeat sent to %+v", hb.BeeId)
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

func pulseTakerId(h HiveId) BeeId {
	return BeeId{
		HiveId:  h,
		AppName: heartbeatAppName,
		Id:      detachedBeeId,
	}
}

func startHeartbeatBee(b BeeId, h Hive) {
	if b.AppName == heartbeatAppName {
		return
	}

	h.SendToBee(startHeartbeat(b), pulseTakerId(h.Id()))
}

func stopHeartbeatBee(b BeeId, h Hive) {
	h.SendToBee(stopHeartbeat(b), pulseTakerId(h.Id()))
}

type heartbeatReqHandler struct{}

func (h *heartbeatReqHandler) Rcv(msg Msg, ctx RcvContext) error {
	if hb, ok := msg.Data().(heartbeatReq); ok {
		ctx.ReplyTo(msg, heartbeatRes(hb))
		return nil
	}

	return fmt.Errorf("An invalid message received: %+v", msg)
}

func (h *heartbeatReqHandler) Map(msg Msg, ctx MapContext) MapSet {
	glog.Fatal("Heartbeat requests should always be unicast.")
	return nil
}
