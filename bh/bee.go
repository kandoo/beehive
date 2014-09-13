package bh

import (
	"encoding/json"
	"runtime/debug"

	"github.com/golang/glog"
)

type BeeID struct {
	HiveID   HiveID  `json:"hive_id"`
	AppName  AppName `json:"app_name"`
	ID       uint64  `json:"id"`
	Detached bool    `json:"detached"`
}

func (b *BeeID) IsNil() bool {
	return len(b.HiveID) == 0 && len(b.AppName) == 0 && b.ID == 0
}

func (b *BeeID) Key() Key {
	return Key(b.Bytes())
}

func (b *BeeID) Bytes() []byte {
	j, err := json.Marshal(b)
	if err != nil {
		glog.Fatalf("Cannot marshall a bee ID into json: %v", err)
	}
	return j
}

func BeeIDFromBytes(b []byte) BeeID {
	id := BeeID{}
	err := json.Unmarshal(b, &id)
	if err != nil {
		glog.Fatalf("Cannot unmarshall a bee ID from json: %v", err)
	}
	return id
}

func BeeIDFromKey(k Key) BeeID {
	return BeeIDFromBytes([]byte(k))
}

type bee interface {
	id() BeeID
	start()

	state() State

	enqueMsg(mh msgAndHandler)
	enqueCmd(cmd LocalCmd)

	handleMsg(mh msgAndHandler)
	// Handles a command and returns false if the bee should stop.
	handleCmd(cmd LocalCmd) bool
}

type localBee struct {
	dataCh chan msgAndHandler
	ctrlCh chan LocalCmd
	ctx    rcvContext
	bID    BeeID
	qee    *qee
}

func (bee *localBee) id() BeeID {
	return bee.bID
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
	glog.Errorf("Error in %s: %v", bee.bID.AppName, err)
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

	bee.ctx.hive.collector.collect(mh.msg.From(), bee.bID, mh.msg)
}

func (bee *localBee) handleCmd(cmd LocalCmd) bool {
	switch cmd.CmdType {
	case stopCmd:
		cmd.ResCh <- CmdResult{}
		return false
	}

	return true
}

func (bee *localBee) enqueMsg(mh msgAndHandler) {
	bee.dataCh <- mh
}

func (bee *localBee) enqueCmd(cmd LocalCmd) {
	bee.ctrlCh <- cmd
}
