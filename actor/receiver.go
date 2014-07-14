package actor

import (
	"encoding/json"

	"github.com/golang/glog"
)

type RcvrId struct {
	StageId   StageId   `json:"stage_id"`
	ActorName ActorName `json:"actor_name"`
	Id        uint32    `json:"id"`
}

func (r *RcvrId) isNil() bool {
	return len(r.StageId) == 0 && len(r.ActorName) == 0 && r.Id == 0
}

func (r *RcvrId) isDetachedId() bool {
	return !r.isNil() && r.Id == detachedRcvrId
}

func (r *RcvrId) Key() Key {
	b, err := json.Marshal(r)
	if err != nil {
		glog.Fatalf("Cannot marshall a receiver ID into json: %v", err)
	}
	return Key(b)
}

func RcvrIdFromKey(k Key) RcvrId {
	r := RcvrId{}
	err := json.Unmarshal([]byte(k), &r)
	if err != nil {
		glog.Fatalf("Cannot unmarshall a receiver ID from json: %v", err)
	}
	return r
}

type receiver interface {
	id() RcvrId
	start()

	state() State

	enqueMsg(mh msgAndHandler)
	enqueCmd(cmd routineCmd)

	handleMsg(mh msgAndHandler)
	// Handles a command and returns false if the receiver should stop.
	handleCmd(cmd routineCmd) bool
}

type localRcvr struct {
	asyncRoutine
	ctx recvContext
	rId RcvrId
}

func (rcvr *localRcvr) id() RcvrId {
	return rcvr.rId
}

func (rcvr *localRcvr) state() State {
	return rcvr.ctx.State()
}

func (rcvr *localRcvr) start() {
	for {
		select {
		case d, ok := <-rcvr.dataCh:
			if !ok {
				return
			}
			rcvr.handleMsg(d)

		case c, ok := <-rcvr.ctrlCh:
			if !ok {
				return
			}
			if ok = rcvr.handleCmd(c); !ok {
				return
			}
		}
	}
}

func (rcvr *localRcvr) handleMsg(mh msgAndHandler) {
	glog.V(2).Infof("Receiver handles a message: %+v", mh.msg)
	mh.handler.Recv(mh.msg, &rcvr.ctx)
	rcvr.ctx.stage.collector.collect(mh.msg.From(), rcvr.rId, mh.msg)
}

func (rcvr *localRcvr) handleCmd(cmd routineCmd) bool {
	switch cmd.cmdType {
	case stopCmd:
		cmd.resCh <- asyncResult{}
		return false
	}

	return true
}

func (rcvr *localRcvr) enqueMsg(mh msgAndHandler) {
	rcvr.dataCh <- mh
}

func (rcvr *localRcvr) enqueCmd(cmd routineCmd) {
	rcvr.ctrlCh <- cmd
}
