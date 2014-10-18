package beehive

import (
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/soheilhy/beehive/Godeps/_workspace/src/github.com/golang/glog"
	"github.com/soheilhy/beehive/state"
)

type bee interface {
	ID() uint64
	colony() Colony

	start()

	State() State
	setState(s State)

	enqueMsg(mh msgAndHandler)
	enqueCmd(cc cmdAndChannel)
}

type localBee struct {
	m sync.Mutex

	beeID     uint64
	beeColony Colony
	stopped   bool
	qee       *qee
	app       *app
	hive      *hive
	timers    []*time.Timer

	dataCh chan msgAndHandler
	ctrlCh chan cmdAndChannel

	state State
	cells map[CellKey]bool
	tx    Tx

	local interface{}
}

func (b *localBee) ID() uint64 {
	return b.beeID
}

func (b *localBee) String() string {
	return fmt.Sprintf("%v/%v/%v", b.hive.ID(), b.app.Name(), b.ID())
}

func (b *localBee) colony() Colony {
	b.m.Lock()
	defer b.m.Unlock()

	return b.beeColony.DeepCopy()
}

func (b *localBee) setColony(c Colony) {
	b.m.Lock()
	defer b.m.Unlock()

	b.beeColony = c
}

func (b *localBee) setState(s State) {
	b.state = s
}

func (b *localBee) start() {
	b.stopped = false
	for !b.stopped {
		select {
		case d, ok := <-b.dataCh:
			if !ok {
				return
			}
			b.handleMsg(d)

		case c, ok := <-b.ctrlCh:
			if !ok {
				return
			}
			b.handleCmd(c)
		}
	}
}

func (b *localBee) recoverFromError(mh msgAndHandler, err interface{},
	stack bool) {
	b.AbortTx()

	if d, ok := err.(time.Duration); ok {
		b.snooze(mh, d)
		return
	}

	glog.Errorf("Error in %s: %v", b.app.Name(), err)
	if stack {
		glog.Errorf("%s", debug.Stack())
	}
}

func (b *localBee) handleMsg(mh msgAndHandler) {
	defer func() {
		if r := recover(); r != nil {
			b.recoverFromError(mh, r, true)
		}
	}()

	glog.V(2).Infof("Bee %v handles a message: %v", b, mh.msg)

	if b.app.Transactional() {
		b.BeginTx()
	}

	if err := mh.handler.Rcv(mh.msg, b); err != nil {
		b.recoverFromError(mh, err, false)
		return
	}

	b.CommitTx()
	// FIXME REFACTOR
	// b.hive.collector.collect(mh.msg.MsgFrom, b.beeID, mh.msg)
}

func (b *localBee) handleCmd(cc cmdAndChannel) {
	glog.V(2).Infof("Bee %v handles command %v", b, cc)

	switch cmd := cc.cmd.Data.(type) {
	case cmdStop:
		b.stop()
		cc.ch <- cmdResult{}

	// FIXME REFACTOR
	//case addMappedCells:
	//b.addMappedCells(cmd.Cells)
	//lcmd.ch <- cmdResult{}

	//case joinColonyCmd:
	//if cmd.Colony.Contains(b.beeID) {
	//b.setColony(cmd.Colony)
	//cc.ch <- cmdResult{}

	//switch {
	//case b.colony().IsSlave(b.beeID):
	//startHeartbeatBee(b.colony().Master, b.hive)

	//case b.colony().IsMaster(b.beeID):
	//for _, s := range b.colony().Slaves {
	//startHeartbeatBee(s, b.hive)
	//}
	//}

	//return
	//}

	//cc.ch <- cmdResult{
	//Err: fmt.Errorf("Bee %v is not in this colony %v", bee, cmd.Colony),
	//}

	//case getColonyCmd:
	//cc.ch <- cmdResult{Data: b.colony()}

	//case addSlaveCmd:
	//var err error
	//slaveID := cmd.BeeID
	//if ok := b.addSlave(slaveID); !ok {
	//err = fmt.Errorf("Slave %v already exists", cmd.BeeID)
	//}
	//cc.ch <- cmdResult{Err: err}

	//case delSlaveCmd:
	//var err error
	//slaveID := cmd.BeeID
	//if ok := b.delSlave(slaveID); !ok {
	//err = fmt.Errorf("Slave %v already exists", cmd.BeeID)
	//}
	//cc.ch <- cmdResult{Err: err}

	//case bufferTxCmd:
	//b.txBuf = append(b.txBuf, cmd.Tx)
	//glog.V(2).Infof("Buffered transaction %v in %v", cmd.Tx, bee)
	//cc.ch <- cmdResult{}

	//case commitTxCmd:
	//seq := cmd.Seq
	//for i, tx := range b.txBuf {
	//if seq == tx.Seq {
	//b.txBuf[i].Status = TxCommitted
	//glog.V(2).Infof("Committed buffered transaction #%d in %v", tx.Seq, bee)
	//cc.ch <- cmdResult{}
	//return
	//}
	//}

	//cc.ch <- cmdResult{Err: fmt.Errorf("Transaction #%d not found.", seq)}

	//case getTxInfoCmd:
	//cc.ch <- cmdResult{
	//Data: b.getTxInfo(),
	//}

	default:
		if cc.ch != nil {
			err := fmt.Errorf("Unknown bee command %#v", cmd)
			glog.Error(err.Error())
			cc.ch <- cmdResult{
				Err: err,
			}
		}
	}
}

func (b *localBee) enqueMsg(mh msgAndHandler) {
	glog.V(3).Infof("Bee %v enqueues message %v", b, mh.msg)
	b.dataCh <- mh
}

func (b *localBee) enqueCmd(cc cmdAndChannel) {
	glog.V(3).Infof("Bee %v enqueues a command %v", b, cc)
	b.ctrlCh <- cc
}

//func (b *localBee) isMaster() bool {
//return b.colony().IsMaster(b.ID())
//}

func (b *localBee) stop() {
	glog.Infof("Bee %v stopped", b.beeID)
	b.stopped = true
	// TODO(soheil): Do we need to stop timers?
}

func (b *localBee) mappedCells() MappedCells {
	b.m.Lock()
	defer b.m.Unlock()

	mc := make(MappedCells, 0, len(b.cells))
	for c := range b.cells {
		mc = append(mc, c)
	}
	return mc
}

func (b *localBee) addMappedCells(cells MappedCells) {
	b.m.Lock()
	defer b.m.Unlock()

	if b.cells == nil {
		b.cells = make(map[CellKey]bool)
	}

	for _, c := range cells {
		glog.V(2).Infof("Adding cell %v to %v", c, b)
		b.cells[c] = true
	}
}

func (b *localBee) addTimer(t *time.Timer) {
	b.m.Lock()
	defer b.m.Unlock()

	b.timers = append(b.timers, t)
}

func (b *localBee) delTimer(t *time.Timer) {
	b.m.Lock()
	defer b.m.Unlock()

	for i := range b.timers {
		if b.timers[i] == t {
			b.timers = append(b.timers[:i], b.timers[i+1:]...)
			return
		}
	}
}

func (b *localBee) snooze(mh msgAndHandler, d time.Duration) {
	t := time.NewTimer(d)
	b.addTimer(t)

	go func() {
		<-t.C
		b.delTimer(t)
		b.enqueMsg(mh)
	}()
}

func (b *localBee) sendCmdToQee(d interface{}) (interface{}, error) {
	ch := make(chan cmdResult)
	b.qee.ctrlCh <- newCmdAndChannel(d, b.App(), 0, ch)
	return (<-ch).get()
}

func (b *localBee) Hive() Hive {
	return b.hive
}

func (b *localBee) State() State {
	return b.state
}

func (b *localBee) Dict(n string) state.Dict {
	return b.State().Dict(n)
}

func (b *localBee) App() string {
	return b.app.Name()
}

// Emits a message. Note that m should be your data not an instance of Msg.
func (b *localBee) Emit(msgData interface{}) {
	b.bufferOrEmit(newMsgFromData(msgData, b.ID(), 0))
}

func (b *localBee) doEmit(msg *msg) {
	b.hive.emitMsg(msg)
}

func (b *localBee) bufferOrEmit(msg *msg) {
	if !b.tx.IsOpen() {
		b.doEmit(msg)
		return
	}

	glog.V(2).Infof("Buffers msg %+v in tx", msg)
	b.tx.AddMsg(msg)
}

func (b *localBee) SendToCellKey(msgData interface{}, to string, k CellKey) {
	// FIXME(soheil): Implement send to.
	glog.Fatal("FIXME implement bee.SendToCellKey")

	msg := newMsgFromData(msgData, b.ID(), 0)
	b.bufferOrEmit(msg)
}

func (b *localBee) SendToBee(msgData interface{}, to uint64) {
	b.bufferOrEmit(newMsgFromData(msgData, b.beeID, to))
}

// Reply to msg with the provided reply.
func (b *localBee) ReplyTo(msg Msg, reply interface{}) error {
	if msg.NoReply() {
		return errors.New("Cannot reply to this message.")
	}

	b.SendToBee(reply, msg.From())
	return nil
}

func (b *localBee) Lock(keys []CellKey) error {
	// FIXME REFACTOR
	//cmd := lockMappedCellsCmd{
	//Keys:   keys,
	//Colony: b.colony(),
	//}
	//_, err := b.sendCmdToQee(cmd)
	//return err
	panic("error FIXME bee.LOCK")
	return nil
}

func (b *localBee) SetBeeLocal(d interface{}) {
	b.local = d
}

func (b *localBee) BeeLocal() interface{} {
	return b.local
}

func (b *localBee) StartDetached(h DetachedHandler) uint64 {
	d, err := b.sendCmdToQee(cmdStartDetached{Handler: h})
	if err != nil {
		glog.Fatalf("Cannot start a detached bee: %v", err)
	}
	return d.(uint64)
}

func (b *localBee) StartDetachedFunc(start StartFunc, stop StopFunc,
	rcv RcvFunc) uint64 {

	return b.StartDetached(&funcDetached{start, stop, rcv})
}

func (b *localBee) BeginTx() error {
	if b.tx.IsOpen() {
		return errors.New("Another tx is open.")
	}

	if err := b.state.BeginTx(); err != nil {
		glog.Errorf("Cannot begin a transaction for %v: %v", b, err)
		return err
	}

	b.tx.Status = state.TxOpen
	glog.V(2).Infof("Bee %v begins transaction #%v", b, b.tx)
	return nil
}

func (b *localBee) emitTxMsgs() {
	if b.tx.Msgs == nil {
		return
	}

	for _, m := range b.tx.Msgs {
		b.doEmit(m.(*msg))
	}
}

func (b *localBee) doCommitTx() error {
	b.emitTxMsgs()
	if err := b.state.CommitTx(); err != nil {
		return err
	}

	b.tx.Reset()
	return nil
}

func (b *localBee) CommitTx() error {
	if !b.tx.IsOpen() {
		return nil
	}

	defer b.tx.Reset()

	// No need to update sequences.
	if b.app.ReplicationFactor() < 2 {
		b.doCommitTx()
		return nil
	}

	panic("FIXME we need to do this through raft")

	//b.tx.Ops = b.txState().Tx()
	//if b.tx.IsEmpty() {
	//b.tx.Seq--
	//return b.doCommitTx()
	//}

	//colony := b.colony()
	//if len(colony.Slaves) < b.app.CommitThreshold() {
	//if err := b.tryToRecruitSlaves(); err != nil {
	//glog.Errorf("Cannot create enough slaves to commit the transaction: %v",
	//err)
	//b.AbortTx()
	//return fmt.Errorf("Not enough slaves to commit the transaction: %v", err)
	//}
	//}

	//b.tx.Generation = colony.Generation

	//retries := 5
	//for {
	//lives, deads := b.replicateTxOnAllSlaves(b.tx)
	//if len(lives) >= b.app.CommitThreshold() {
	//break
	//}

	//glog.Warningf("Replicated less than commit threshold %v", len(lives))

	//if retries == 0 {
	//// TODO(soheil): Should we really fail here?
	//b.AbortTx()
	//return fmt.Errorf("Can only replicate to %v slaves", len(lives))
	//}
	//retries--
	//time.Sleep(5 * time.Millisecond)
	//for _, s := range deads {
	//glog.V(2).Infof("Trying to replace slave %v", s)
	//b.handleSlaveFailure(s)
	//}
	//glog.V(2).Infof("Allocated new slaves will retry")
	//}

	//if err := b.doCommitTx(); err != nil {
	//glog.Fatalf("Error in committing the transaction: %v", err)
	//}

	//b.txBuf = append(b.txBuf, b.tx)

	//if err := b.sendCommitToAllSlaves(b.tx.Seq); err != nil {
	//glog.Errorf("Cannot notify all salves about transaction: %v", err)
	//}

	//glog.V(2).Infof("Bee %v committed tx #%v", b, b.tx.Seq)
	return nil
}

func (b *localBee) AbortTx() error {
	if !b.tx.IsOpen() {
		return nil
	}

	glog.V(2).Infof("Bee %v aborts tx", b)
	b.tx.Reset()
	return b.state.AbortTx()
}

func (bee *localBee) Snooze(d time.Duration) {
	panic(d)
}
