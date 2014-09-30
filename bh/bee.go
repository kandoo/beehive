package bh

import (
	"bytes"
	"encoding/json"
	"fmt"
	"runtime/debug"
	"strconv"
	"sync"
	"time"

	"github.com/golang/glog"
)

type BeeID struct {
	HiveID   HiveID  `json:"hive_id"`
	AppName  AppName `json:"app_name"`
	ID       uint64  `json:"id"`
	Detached bool    `json:"detached"`
}

func (b *BeeID) IsNil() bool {
	return b.ID == 0 && len(b.HiveID) == 0 && len(b.AppName) == 0
}

func (b BeeID) Key() Key {
	return Key(b.String())
}

func (b BeeID) String() string {
	if b.IsNil() {
		return "*"
	}

	var buf bytes.Buffer
	buf.Grow(len(b.HiveID) + len(b.AppName) + 2)
	buf.WriteString(string(b.HiveID))
	buf.WriteString("/")
	buf.WriteString(string(b.AppName))
	buf.WriteString("/")

	if b.ID == 0 {
		buf.WriteString("Q")
		return buf.String()
	}

	return string(strconv.AppendUint(buf.Bytes(), b.ID, 10))
}

func (b *BeeID) Bytes() []byte {
	j, err := json.Marshal(b)
	if err != nil {
		glog.Fatalf("Cannot marshall a bee ID into json: %v", err)
	}
	return j
}

func (b BeeID) queen() BeeID {
	b.ID = 0
	return b
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

func (c BeeColony) String() string {
	return fmt.Sprintf("%v (%v gen:%d)", c.Master, c.Slaves, c.Generation)
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
	if c.IsMaster(id) {
		return false
	}

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

func (c BeeColony) DeepCopy() BeeColony {
	slaves := make([]BeeID, len(c.Slaves))
	copy(slaves, c.Slaves)
	c.Slaves = slaves
	return c
}

func (c BeeColony) Equal(thatC BeeColony) bool {
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

func (c *BeeColony) SlaveHives() []HiveID {
	slaves := make([]HiveID, 0, len(c.Slaves))
	for _, s := range c.Slaves {
		slaves = append(slaves, s.HiveID)
	}
	return slaves
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
	colony() BeeColony
	slaves() []BeeID

	start()

	State() State
	txState() TxState
	setState(s TxState)
	txBuffer() []Tx
	setTxBuffer(txs []Tx)

	enqueMsg(mh msgAndHandler)
	enqueCmd(cmd LocalCmd)

	handleMsg(mh msgAndHandler)
	handleCmd(cmd LocalCmd)
}

type localBee struct {
	mutex     sync.Mutex
	beeID     BeeID
	beeColony BeeColony
	stopped   bool
	qee       *qee
	app       *app
	hive      *hive
	timers    []*time.Timer

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

func (bee *localBee) String() string {
	return bee.beeID.String()
}

func (bee *localBee) gen() TxGeneration {
	bee.mutex.Lock()
	defer bee.mutex.Unlock()

	return bee.beeColony.Generation
}

func (bee *localBee) colony() BeeColony {
	bee.mutex.Lock()
	defer bee.mutex.Unlock()

	return bee.beeColony.DeepCopy()
}

func (bee *localBee) generation() TxGeneration {
	bee.mutex.Lock()
	defer bee.mutex.Unlock()

	return bee.beeColony.Generation
}

func (bee *localBee) setColony(c BeeColony) {
	bee.mutex.Lock()
	defer bee.mutex.Unlock()

	bee.beeColony = c
}

func (bee *localBee) slaves() []BeeID {
	return bee.colony().Slaves
}

func (bee *localBee) addSlave(s BeeID) bool {
	bee.mutex.Lock()
	defer bee.mutex.Unlock()

	return bee.beeColony.AddSlave(s)
}

func (bee *localBee) delSlave(s BeeID) bool {
	bee.mutex.Lock()
	defer bee.mutex.Unlock()

	return bee.beeColony.DelSlave(s)
}

func (bee *localBee) setState(s TxState) {
	bee.state = s
}

func (bee *localBee) setTxBuffer(txs []Tx) {
	bee.txBuf = txs
}

func (bee *localBee) txBuffer() []Tx {
	return bee.txBuf
}

func (bee *localBee) start() {
	bee.stopped = false
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
	bee.AbortTx()

	if d, ok := err.(time.Duration); ok {
		bee.snooze(mh, d)
		return
	}

	glog.Errorf("Error in %s: %v", bee.beeID.AppName, err)
	if stack {
		glog.Errorf("%s", debug.Stack())
	}
}

func (bee *localBee) handleMsg(mh msgAndHandler) {
	defer func() {
		if r := recover(); r != nil {
			bee.recoverFromError(mh, r, true)
		}
	}()

	glog.V(2).Infof("Bee %v handles a message: %v", bee, mh.msg)

	if bee.app.Transactional() {
		bee.BeginTx()
	}

	if err := mh.handler.Rcv(mh.msg, bee); err != nil {
		bee.recoverFromError(mh, err, false)
		return
	}

	bee.CommitTx()
	bee.hive.collector.collect(mh.msg.MsgFrom, bee.beeID, mh.msg)
}

func (bee *localBee) handleCmd(lcmd LocalCmd) {
	glog.V(2).Infof("Bee %v handles command %v", bee, lcmd)

	switch cmd := lcmd.Cmd.(type) {
	case stopCmd:
		bee.stop()
		lcmd.ResCh <- CmdResult{}

	case addMappedCells:
		bee.addMappedCells(cmd.Cells)
		lcmd.ResCh <- CmdResult{}

	case joinColonyCmd:
		if cmd.Colony.Contains(bee.beeID) {
			bee.setColony(cmd.Colony)
			lcmd.ResCh <- CmdResult{}

			switch {
			case bee.colony().IsSlave(bee.beeID):
				startHeartbeatBee(bee.colony().Master, bee.hive)

			case bee.colony().IsMaster(bee.beeID):
				for _, s := range bee.colony().Slaves {
					startHeartbeatBee(s, bee.hive)
				}
			}

			return
		}

		lcmd.ResCh <- CmdResult{
			Err: fmt.Errorf("Bee %v is not in this colony %v", bee, cmd.Colony),
		}

	case getColonyCmd:
		lcmd.ResCh <- CmdResult{Data: bee.colony()}

	case addSlaveCmd:
		var err error
		slaveID := cmd.BeeID
		if ok := bee.addSlave(slaveID); !ok {
			err = fmt.Errorf("Slave %v already exists", cmd.BeeID)
		}
		lcmd.ResCh <- CmdResult{Err: err}

	case delSlaveCmd:
		var err error
		slaveID := cmd.BeeID
		if ok := bee.delSlave(slaveID); !ok {
			err = fmt.Errorf("Slave %v already exists", cmd.BeeID)
		}
		lcmd.ResCh <- CmdResult{Err: err}

	case bufferTxCmd:
		bee.txBuf = append(bee.txBuf, cmd.Tx)
		glog.V(2).Infof("Buffered transaction %v in %v", cmd.Tx, bee)
		lcmd.ResCh <- CmdResult{}

	case commitTxCmd:
		seq := cmd.Seq
		for i, tx := range bee.txBuf {
			if seq == tx.Seq {
				bee.txBuf[i].Status = TxCommitted
				glog.V(2).Infof("Committed buffered transaction #%d in %v", tx.Seq, bee)
				lcmd.ResCh <- CmdResult{}
				return
			}
		}

		lcmd.ResCh <- CmdResult{Err: fmt.Errorf("Transaction #%d not found.", seq)}

	case getTxInfoCmd:
		lcmd.ResCh <- CmdResult{
			Data: bee.getTxInfo(),
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
	glog.V(3).Infof("Bee %v enqueues message %v", bee, mh.msg)
	bee.dataCh <- mh
}

func (bee *localBee) enqueCmd(cmd LocalCmd) {
	glog.V(3).Infof("Bee %v enqueues a command %v", bee, cmd)
	bee.ctrlCh <- cmd
}

func (bee *localBee) isMaster() bool {
	return bee.colony().IsMaster(bee.beeID)
}

func (bee *localBee) stop() {
	glog.Infof("Bee %s stopped", bee.beeID)
	bee.stopped = true
	// TODO(soheil): Do we need to stop timers?
}

func (bee *localBee) mappedCells() MappedCells {
	bee.mutex.Lock()
	defer bee.mutex.Unlock()

	mc := make(MappedCells, 0, len(bee.cells))
	for c := range bee.cells {
		mc = append(mc, c)
	}
	return mc
}

func (bee *localBee) addMappedCells(cells MappedCells) {
	bee.mutex.Lock()
	defer bee.mutex.Unlock()

	if bee.cells == nil {
		bee.cells = make(map[CellKey]bool)
	}

	for _, c := range cells {
		glog.V(2).Infof("Adding cell %v to %v", c, bee)
		bee.cells[c] = true
	}
}

func (bee *localBee) getTxInfo() TxInfo {
	info := TxInfo{
		Generation: bee.gen(),
	}
	if len(bee.txBuf) != 0 {
		info.LastBuffered = bee.txBuf[len(bee.txBuf)-1].Seq
	}
	if _, tx := bee.lastCommittedTx(); tx != nil {
		info.LastCommitted = tx.Seq
	}
	return info
}

func (bee *localBee) lastCommittedTx() (int, *Tx) {
	for i := len(bee.txBuf) - 1; i >= 0; i-- {
		if bee.txBuf[i].Status == TxCommitted {
			return i, &bee.txBuf[i]
		}
	}

	return -1, nil
}

func (bee *localBee) addTimer(t *time.Timer) {
	bee.mutex.Lock()
	defer bee.mutex.Unlock()

	bee.timers = append(bee.timers, t)
}

func (bee *localBee) delTimer(t *time.Timer) {
	bee.mutex.Lock()
	defer bee.mutex.Unlock()

	for i := range bee.timers {
		if bee.timers[i] == t {
			bee.timers = append(bee.timers[:i], bee.timers[i+1:]...)
			return
		}
	}
}

func (bee *localBee) snooze(mh msgAndHandler, d time.Duration) {
	t := time.NewTimer(d)
	bee.addTimer(t)

	go func() {
		<-t.C
		bee.delTimer(t)
		bee.enqueMsg(mh)
	}()
}

func (bee *localBee) commitAllBufferedTxs() {
	if !bee.isMaster() {
		return
	}

	if len(bee.txBuf) == 0 {
		return
	}

	commitIndex, _ := bee.lastCommittedTx()
	if commitIndex == len(bee.txBuf) {
		return
	}

	txs := append([]Tx{}, bee.txBuf[commitIndex+1:]...)
	if commitIndex < 0 {
		bee.txBuf = make([]Tx, 0, len(txs))
	} else {
		bee.txBuf = bee.txBuf[:commitIndex]
	}

	for _, tx := range txs {
		bee.tx = tx
		bee.CommitTx()
	}
}
