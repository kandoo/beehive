package bh

import (
	"time"

	"github.com/golang/glog"
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

type beeFailed struct {
	id BeeId
}

type stopHeartbeating struct {
	resCh chan interface{}
}

type heartbeatHandler struct {
	ctrlCh        chan interface{}
	dataCh        chan Msg
	hbMap         map[BeeId]heartbeat
	queryInterval time.Duration
	deadInterval  time.Duration
}

func (h *heartbeatHandler) Start(ctx RcvContext) {
	for {
		select {
		case c := <-h.ctrlCh:
			switch cmd := c.(type) {
			case stopHeartbeating:
				close(h.dataCh)
				close(h.ctrlCh)
				cmd.resCh <- true
				return
			}
		case m := <-h.dataCh:
			h.handleHeartbeat(m, ctx)
		case <-time.After(h.queryInterval):
			h.heartbeatAll(ctx)
		}
	}
}

func (h *heartbeatHandler) handleHeartbeat(msg Msg, ctx RcvContext) {
	switch d := msg.Data().(type) {
	case heartbeatReq:
		ctx.ReplyTo(msg, heartbeatRes(d))
	case heartbeatRes:
		hb, ok := h.hbMap[d.BeeId]
		if !ok {
			glog.Errorf("Received heartbeat response that we have not requested: %+v",
				d.BeeId)
		}
		hb.TimeStamp = time.Now().Unix()
	case startHeartbeat:
		h.hbMap[BeeId(d)] = heartbeat{BeeId: BeeId(d)}
	}
}

func (h *heartbeatHandler) heartbeatAll(ctx RcvContext) {
	now := time.Now().Unix()
	for _, hb := range h.hbMap {
		if now-hb.TimeStamp > int64(h.deadInterval*time.Millisecond) {
			glog.Infof("A bee is found dead: %+v", hb.BeeId)
			ctx.Emit(beeFailed{id: hb.BeeId})
			continue
		}

		hb.TimeStamp = time.Now().Unix()
		// TODO(soheil): There might be a problem here. What if SendToBee blocks
		// because of an overwhelmed channel. Shouldn't we update "now"?
		ctx.SendToBee(heartbeatReq(hb), hb.BeeId)
	}
}

func (h *heartbeatHandler) Stop(ctx RcvContext) {
	ch := make(chan interface{})
	h.ctrlCh <- stopHeartbeating{ch}
	<-ch
}

func (h *heartbeatHandler) Rcv(m Msg, ctx RcvContext) {
	h.dataCh <- m
}
