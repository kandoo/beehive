package bh

import (
	"encoding/json"
	"runtime/debug"

	"github.com/golang/glog"
)

type BeeId struct {
	HiveId  HiveId  `json:"hive_id"`
	AppName AppName `json:"app_name"`
	Id      uint32  `json:"id"`
}

func (b *BeeId) isNil() bool {
	return len(b.HiveId) == 0 && len(b.AppName) == 0 && b.Id == 0
}

func (b *BeeId) isDetachedId() bool {
	return !b.isNil() && b.Id == detachedBeeId
}

func (b *BeeId) Key() Key {
	j, err := json.Marshal(b)
	if err != nil {
		glog.Fatalf("Cannot marshall a bee ID into json: %v", err)
	}
	return Key(j)
}

func BeeIdFromKey(k Key) BeeId {
	b := BeeId{}
	err := json.Unmarshal([]byte(k), &b)
	if err != nil {
		glog.Fatalf("Cannot unmarshall a bee ID from json: %v", err)
	}
	return b
}

type bee interface {
	id() BeeId
	start()

	state() State

	enqueMsg(mh msgAndHandler)
	enqueCmd(cmd routineCmd)

	handleMsg(mh msgAndHandler)
	// Handles a command and returns false if the bee should stop.
	handleCmd(cmd routineCmd) bool
}

type localBee struct {
	asyncRoutine
	ctx rcvContext
	rId BeeId
}

func (bee *localBee) id() BeeId {
	return bee.rId
}

func (bee *localBee) state() State {
	return bee.ctx.State()
}

func (bee *localBee) start() {
	for {
		select {
		case d, ok := <-bee.dataCh:
			if !ok {
				return
			}
			bee.handleMsg(d)

		case c, ok := <-bee.ctrlCh:
			if !ok {
				return
			}
			if ok = bee.handleCmd(c); !ok {
				return
			}
		}
	}
}

func (bee *localBee) recoverFromError(mh msgAndHandler, err interface{},
	stack bool) {
	glog.Errorf("Error in %s: %v", bee.rId.AppName, err)
	if stack {
		glog.Errorf("%s", debug.Stack())
	}

	bee.ctx.State().AbortTx()
}

func (bee *localBee) handleMsg(mh msgAndHandler) {
	defer func() {
		if r := recover(); r != nil {
			bee.recoverFromError(mh, r, true)
		}
	}()

	glog.V(2).Infof("Bee handles a message: %+v", mh.msg)

	bee.ctx.State().BeginTx()
	if err := mh.handler.Rcv(mh.msg, &bee.ctx); err != nil {
		bee.recoverFromError(mh, err, false)
		return
	}
	bee.ctx.State().CommitTx()

	bee.ctx.hive.collector.collect(mh.msg.From(), bee.rId, mh.msg)
}

func (bee *localBee) handleCmd(cmd routineCmd) bool {
	switch cmd.cmdType {
	case stopCmd:
		cmd.resCh <- asyncResult{}
		return false
	}

	return true
}

func (bee *localBee) enqueMsg(mh msgAndHandler) {
	bee.dataCh <- mh
}

func (bee *localBee) enqueCmd(cmd routineCmd) {
	bee.ctrlCh <- cmd
}
