package bh

import (
	"encoding/json"
	"fmt"
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

func (b *BeeID) String() string {
	return string(b.Bytes())
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

type BeeColony struct {
	Master BeeID   `json:"master"`
	Slaves []BeeID `json:"slaves"`
}

func (c *BeeColony) AddSlave(id BeeID) bool {
	for _, s := range c.Slaves {
		if s == id {
			return false
		}
	}
	c.Slaves = append(c.Slaves, id)
	return true
}

func (c *BeeColony) DelSlave(id BeeID) bool {
	for i, s := range c.Slaves {
		if s == id {
			c.Slaves = append(c.Slaves[:i], c.Slaves[i+1:]...)
			return true
		}
	}

	return false
}

func (c BeeColony) Eq(thatC BeeColony) bool {
	if c.Master != thatC.Master {
		return false
	}

	if len(c.Slaves) != len(thatC.Slaves) {
		return false
	}

	if len(c.Slaves) == 0 && len(thatC.Slaves) == 0 {
		return true
	}

	slaves := make(map[BeeID]bool)
	for _, b := range c.Slaves {
		slaves[b] = true
	}

	for _, b := range thatC.Slaves {
		if _, ok := slaves[b]; !ok {
			return false
		}
	}

	return true
}

func (c *BeeColony) Bytes() ([]byte, error) {
	j, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}
	return j, nil
}

func BeeColonyFromBytes(b []byte) (BeeColony, error) {
	c := BeeColony{}
	err := json.Unmarshal(b, &c)
	return c, err
}

type bee interface {
	id() BeeID
	slaves() []BeeID
	colonyUnsafe() BeeColony

	start()

	state() State
	setState(s State)

	enqueMsg(mh msgAndHandler)
	enqueCmd(cmd LocalCmd)

	handleMsg(mh msgAndHandler)
	// Handles a command and returns false if the bee should stop.
	handleCmd(cmd LocalCmd) bool
}

type localBee struct {
	dataCh    chan msgAndHandler
	ctrlCh    chan LocalCmd
	ctx       rcvContext
	beeColony BeeColony
	qee       *qee
}

func (bee *localBee) id() BeeID {
	return bee.beeColony.Master
}

func (bee *localBee) colonyUnsafe() BeeColony {
	return bee.beeColony
}

func (bee *localBee) slaves() []BeeID {
	resCh := make(chan CmdResult)
	bee.enqueCmd(LocalCmd{
		CmdType: listSlavesCmd,
		ResCh:   resCh,
	})

	d, err := (<-resCh).get()
	if err != nil {
		glog.Errorf("Error in list slaves: %v", err)
		return nil
	}

	return d.([]BeeID)
}

func (bee *localBee) state() State {
	return bee.ctx.State()
}

func (bee *localBee) setState(s State) {
	bee.ctx.state = s
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
	glog.Errorf("Error in %s: %v", bee.id().AppName, err)
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

	if bee.ctx.app.Transactional() {
		bee.ctx.State().BeginTx()
	}

	if err := mh.handler.Rcv(mh.msg, &bee.ctx); err != nil {
		bee.recoverFromError(mh, err, false)
		return
	}

	bee.ctx.State().CommitTx()

	bee.ctx.hive.collector.collect(mh.msg.From(), bee.id(), mh.msg)
}

func (bee *localBee) handleCmd(cmd LocalCmd) bool {
	switch cmd.CmdType {
	case stopCmd:
		cmd.ResCh <- CmdResult{}
		return false

	case listSlavesCmd:
		cmd.ResCh <- CmdResult{Data: bee.beeColony.Slaves}

	case addSlaveCmd:
		var err error
		slaveID := cmd.CmdData.(addSlaveCmdData).BeeID
		if ok := bee.beeColony.AddSlave(slaveID); !ok {
			err = fmt.Errorf("Slave %s already exists", cmd.CmdData.(BeeID))
		}
		cmd.ResCh <- CmdResult{Err: err}

	case delSlaveCmd:
		var err error
		slaveID := cmd.CmdData.(delSlaveCmdData).BeeID
		if ok := bee.beeColony.DelSlave(slaveID); !ok {
			err = fmt.Errorf("Slave %s already exists", cmd.CmdData.(BeeID))
		}
		cmd.ResCh <- CmdResult{Err: err}

	default:
		if cmd.ResCh != nil {
			glog.Errorf("Unknown bee command %v", cmd)
			cmd.ResCh <- CmdResult{
				Err: fmt.Errorf("Unknown bee command %v", cmd),
			}
		}
	}
	return true
}

func (bee *localBee) enqueMsg(mh msgAndHandler) {
	glog.V(2).Infof("Enqueue message %+v in bee %+v", mh.msg, bee)
	bee.dataCh <- mh
}

func (bee *localBee) enqueCmd(cmd LocalCmd) {
	glog.V(2).Infof("Enqueue a command %+v in bee %+v", cmd, bee)
	bee.ctrlCh <- cmd
}
