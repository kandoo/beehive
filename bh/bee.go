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
	Master     BeeID        `json:"master"`
	Slaves     []BeeID      `json:"slaves"`
	Generation TxGeneration `json:"generation"`
}

func (c BeeColony) IsNil() bool {
	return c.Master.IsNil()
}

func (c BeeColony) IsMaster(id BeeID) bool {
	return c.Master == id
}

func (c BeeColony) IsSlave(id BeeID) bool {
	for _, s := range c.Slaves {
		if s == id {
			return true
		}
	}
	return false
}

func (c BeeColony) Contains(id BeeID) bool {
	return c.IsMaster(id) || c.IsSlave(id)
}

func (c *BeeColony) AddSlave(id BeeID) bool {
	if c.IsSlave(id) {
		return false
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
	colony() BeeColony
	colonyUnsafe() BeeColony

	start()

	State() State
	txState() TxState
	setState(s TxState)

	enqueMsg(mh msgAndHandler)
	enqueCmd(cmd LocalCmd)

	handleMsg(mh msgAndHandler)
	handleCmd(cmd LocalCmd)

	replicateTx(tx *Tx) error
	notifyCommitTx(tx TxSeq) error
}

type localBee struct {
	beeID     BeeID
	beeColony BeeColony
	stopped   bool
	qee       *qee
	app       *app
	hive      *hive

	dataCh chan msgAndHandler
	ctrlCh chan LocalCmd

	state TxState
	cells map[CellKey]bool
	txBuf []Tx
	tx    Tx

	local interface{}
}

func (bee *localBee) id() BeeID {
	return bee.beeID
}

func (bee *localBee) colonyUnsafe() BeeColony {
	return bee.beeColony
}

func (bee *localBee) colony() BeeColony {
	resCh := make(chan CmdResult)
	bee.enqueCmd(NewLocalCmd(getColonyCmd{}, BeeID{}, resCh))
	d, err := (<-resCh).get()
	if err != nil {
		glog.Errorf("Error in getting the bee colony: %v", err)
		return BeeColony{}
	}

	return d.(BeeColony)
}

func (bee *localBee) slaves() []BeeID {
	return bee.colony().Slaves
}

func (bee *localBee) setState(s TxState) {
	bee.state = s
}

func (bee *localBee) start() {
	for !bee.stopped {
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
			bee.handleCmd(c)
		}
	}
}

func (bee *localBee) recoverFromError(mh msgAndHandler, err interface{},
	stack bool) {
	glog.Errorf("Error in %s: %v", bee.id().AppName, err)
	if stack {
		glog.Errorf("%s", debug.Stack())
	}

	bee.AbortTx()
}

func (bee *localBee) handleMsg(mh msgAndHandler) {
	defer func() {
		if r := recover(); r != nil {
			bee.recoverFromError(mh, r, true)
		}
	}()

	glog.V(2).Infof("Bee handles a message: %#v", mh.msg)

	if bee.app.Transactional() {
		bee.BeginTx()
	}

	if err := mh.handler.Rcv(mh.msg, bee); err != nil {
		bee.recoverFromError(mh, err, false)
		return
	}

	bee.CommitTx()

	bee.hive.collector.collect(mh.msg.From(), bee.id(), mh.msg)
}

func (bee *localBee) handleCmd(lcmd LocalCmd) {
	glog.V(2).Infof("Bee %#v handles a command %#v", bee, lcmd.Cmd)

	switch cmd := lcmd.Cmd.(type) {
	case stopCmd:
		bee.stop()
		lcmd.ResCh <- CmdResult{}

	case addMappedCell:
		if bee.cells == nil {
			bee.cells = make(map[CellKey]bool)
		}

		for _, c := range cmd.Cells {
			bee.cells[c] = true
		}

	case joinColonyCmd:
		if cmd.Colony.Contains(bee.id()) {
			bee.beeColony = cmd.Colony
			lcmd.ResCh <- CmdResult{}

			switch {
			case bee.beeColony.IsSlave(bee.id()):
				startHeartbeatBee(bee.beeColony.Master, bee.hive)

			case bee.beeColony.IsMaster(bee.id()):
				for _, s := range bee.beeColony.Slaves {
					startHeartbeatBee(s, bee.hive)
				}
			}

			return
		}

		lcmd.ResCh <- CmdResult{
			Err: fmt.Errorf("Bee %#v is not in this colony %#v", bee.id(),
				cmd.Colony),
		}

	case getColonyCmd:
		lcmd.ResCh <- CmdResult{Data: bee.beeColony}

	case addSlaveCmd:
		var err error
		slaveID := cmd.BeeID
		if ok := bee.beeColony.AddSlave(slaveID); !ok {
			err = fmt.Errorf("Slave %s already exists", cmd.BeeID)
		}
		lcmd.ResCh <- CmdResult{Err: err}

	case delSlaveCmd:
		var err error
		slaveID := cmd.BeeID
		if ok := bee.beeColony.DelSlave(slaveID); !ok {
			err = fmt.Errorf("Slave %s already exists", cmd.BeeID)
		}
		lcmd.ResCh <- CmdResult{Err: err}

	case bufferTxCmd:
		tx := cmd.Tx
		bee.txBuf = append(bee.txBuf, tx)
		glog.V(2).Infof("Buffered transaction #%d in %#v", tx.Seq, bee.id())
		lcmd.ResCh <- CmdResult{}

	case commitTxCmd:
		seq := cmd.Seq
		for _, tx := range bee.txBuf {
			if seq == tx.Seq {
				tx.Status = TxCommitted
				glog.V(2).Infof("Committed buffered transaction #%d in %#v", tx.Seq,
					bee.id())
				lcmd.ResCh <- CmdResult{}
			}
		}
		lcmd.ResCh <- CmdResult{Err: fmt.Errorf("Transaction #%d not found.", seq)}

	case getTxInfoCmd:
		info := TxInfo{
			Generation:    bee.beeColony.Generation,
			LastBuffered:  bee.txBuf[len(bee.txBuf)-1].Seq,
			LastCommitted: bee.lastCommittedTx().Seq,
		}
		lcmd.ResCh <- CmdResult{
			Data: info,
		}

	default:
		if lcmd.ResCh != nil {
			err := fmt.Errorf("Unknown bee command %#v", cmd)
			glog.Error(err.Error())
			lcmd.ResCh <- CmdResult{
				Err: err,
			}
		}
	}
}

func (bee *localBee) enqueMsg(mh msgAndHandler) {
	glog.V(2).Infof("Enqueue message %#v in bee %#v", mh.msg, bee.id())
	bee.dataCh <- mh
}

func (bee *localBee) enqueCmd(cmd LocalCmd) {
	glog.V(2).Infof("Enqueue a command %#v in bee %#v", cmd, bee.id())
	bee.ctrlCh <- cmd
}

func (bee *localBee) isMaster() bool {
	return bee.beeColony.IsMaster(bee.id())
}

func (bee *localBee) replicateTx(tx *Tx) error {
	// TODO(soheil): Add a commit threshold.
	if !bee.isMaster() {
		return fmt.Errorf("Bee %#v is not a master of %#v", bee.id(), bee.beeColony)
	}

	for i, s := range bee.beeColony.Slaves {
		prx := NewProxy(s.HiveID)
		cmd := NewRemoteCmd(bufferTxCmd{*tx}, s)
		_, err := prx.SendCmd(&cmd)
		if err != nil {
			glog.Errorf("Cannot replicate tx %#v on bee %#v", tx, s)
		}

		if err != nil && i == 0 {
			return err
		}
	}

	return nil
}

func (bee *localBee) notifyCommitTx(tx TxSeq) error {
	var ret error
	for _, s := range bee.beeColony.Slaves {
		prx := NewProxy(s.HiveID)
		cmd := NewRemoteCmd(commitTxCmd{tx}, s)
		_, err := prx.SendCmd(&cmd)
		if err != nil {
			ret = err
		}
	}
	return ret
}

func (bee *localBee) stop() {
	glog.Infof("Bee %#v stopped", bee.id())
	bee.stopped = true
}

func (bee *localBee) mappedCells() MappedCells {
	mc := make(MappedCells, 0, len(bee.cells))
	for c := range bee.cells {
		mc = append(mc, c)
	}
	return mc
}

func (bee *localBee) lastCommittedTx() *Tx {
	for i := len(bee.txBuf) - 1; i >= 0; i-- {
		if bee.txBuf[i].Status == TxCommitted {
			return &bee.txBuf[i]
		}
	}

	return nil
}
