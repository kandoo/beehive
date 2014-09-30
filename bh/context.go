package bh

import (
	"errors"
	"fmt"
	"time"

	"github.com/golang/glog"
)

type Context interface {
	// Hive returns the Hive of this context.
	Hive() Hive
	// App returns the name of the application of this context.
	App() AppName
	// State returns the state of the bee/queen bee.
	State() State
	// Dict is a helper function that returns the specific dict within the state.
	Dict(n DictName) Dict
}

// MapContext is passed to the map functions of message handlers. It provides
// all the platform-level functions required to implement the map function.
type MapContext interface {
	Context

	// Local returns a mapped cell unique to the hive of this map context.
	LocalCells() MappedCells
}

// RcvContext is passed to the rcv functions of message handlers. It provides
// all the platform-level functions required to implement the rcv function.
type RcvContext interface {
	Context

	// BeeID returns the BeeID in this context.
	BeeID() BeeID

	// Emit emits a message.
	Emit(msgData interface{})
	// SendToCellKey sends a message to the bee of the give app that owns the
	// given cell key.
	SendToCellKey(msgData interface{}, to AppName, dk CellKey)
	// SendToBee sends a message to the given bee.
	SendToBee(msgData interface{}, to BeeID)
	// ReplyTo replies to a message: Sends a message from the current bee to the
	// bee that emitted msg.
	ReplyTo(msg Msg, replyData interface{}) error

	// StartDetached spawns a detached handler.
	StartDetached(h DetachedHandler) BeeID
	// StartDetachedFunc spawns a detached handler using the provide function.
	StartDetachedFunc(start StartFunc, stop StopFunc, rcv RcvFunc) BeeID

	// Lock proactively locks the cells in the given mapped cells.
	Lock(ms MappedCells) error

	// Snooze exits the Rcv function, and schedules the current message to be
	// enqued again after at least duration d.
	Snooze(d time.Duration)

	// BeeLocal returns the bee-local storage. It is an ephemeral memory that is
	// just visible to the current bee. Very similar to thread-locals in the scope
	// of a bee.
	BeeLocal() interface{}
	// SetBeeLocal sets a data in the bee-local storage.
	SetBeeLocal(d interface{})

	// Starts a transaction in this context. Transactions span multiple
	// dictionaries and buffer all messages. When a transaction commits all the
	// side effects will be applied. Note that since handlers are called in a
	// single bee, transactions are mostly for programming convinience and easy
	// atomocity.
	BeginTx() error
	// Commits the current transaction.
	// If the application has a 2+ replication factor, calling commit also means
	// that we will wait until the transaction is sufficiently replicated and then
	// commits the transaction.
	CommitTx() error
	// Aborts the transaction.
	AbortTx() error
}

type mapContext struct {
	state TxState
	hive  *hive
	app   *app
}

func (q *qee) State() State {
	if q.state == nil {
		q.state = newState(q.app)
	}

	return q.state
}

func (q *qee) Dict(n DictName) Dict {
	return q.State().Dict(n)
}

func (q *qee) Hive() Hive {
	return q.hive
}

func (q *qee) LocalCells() MappedCells {
	return MappedCells{{"__nil_dict__", Key(q.hive.ID())}}
}

func (q *qee) App() AppName {
	return q.app.Name()
}

func (b *localBee) Hive() Hive {
	return b.hive
}

func (b *localBee) State() State {
	return b.txState()
}

func (b *localBee) Dict(n DictName) Dict {
	return b.State().Dict(n)
}

func (b *localBee) App() AppName {
	return b.beeID.AppName
}

// Emits a message. Note that m should be your data not an instance of Msg.
func (b *localBee) Emit(msgData interface{}) {
	b.bufferOrEmit(newMsgFromData(msgData, b.id(), BeeID{}))
}

func (b *localBee) doEmit(msg *msg) {
	b.hive.emitMsg(msg)
}

func (b *localBee) bufferOrEmit(msg *msg) {
	if !b.tx.IsOpen() {
		b.doEmit(msg)
		return
	}

	glog.V(2).Infof("Buffers msg %+v in tx %d", msg, b.tx.Seq)
	b.tx.AddMsg(msg)
}

func (b *localBee) SendToCellKey(msgData interface{}, to AppName,
	dk CellKey) {
	// TODO(soheil): Implement send to.
	glog.Fatal("Sendto is not implemented.")

	msg := newMsgFromData(msgData, b.id(), BeeID{})
	b.bufferOrEmit(msg)
}

func (b *localBee) SendToBee(msgData interface{}, to BeeID) {
	b.bufferOrEmit(newMsgFromData(msgData, b.beeID, to))
}

// Reply to thatMsg with the provided replyData.
func (b *localBee) ReplyTo(thatMsg Msg, replyData interface{}) error {
	m := thatMsg.(*msg)
	if m.NoReply() {
		return errors.New("Cannot reply to this message.")
	}

	b.SendToBee(replyData, m.From())
	return nil
}

func (b *localBee) Lock(ms MappedCells) error {
	resCh := make(chan CmdResult)
	cmd := lockMappedCellsCmd{
		MappedCells: ms,
		Colony:      b.colony(),
	}
	b.qee.ctrlCh <- NewLocalCmd(cmd, BeeID{}, resCh)
	res := <-resCh
	return res.Err
}

func (b *localBee) SetBeeLocal(d interface{}) {
	b.local = d
}

func (b *localBee) BeeLocal() interface{} {
	return b.local
}

func (b *localBee) StartDetached(h DetachedHandler) BeeID {
	resCh := make(chan CmdResult)
	cmd := NewLocalCmd(startDetachedCmd{Handler: h}, BeeID{}, resCh)
	b.qee.ctrlCh <- cmd
	return (<-resCh).Data.(BeeID)
}

func (b *localBee) StartDetachedFunc(start StartFunc, stop StopFunc,
	rcv RcvFunc) BeeID {

	return b.StartDetached(&funcDetached{start, stop, rcv})
}

func (b *localBee) BeeID() BeeID {
	return b.id()
}

func (b *localBee) txState() TxState {
	if b.state == nil {
		b.state = newState(b.app)
	}

	return b.state
}

func (b *localBee) BeginTx() error {
	if b.tx.IsOpen() {
		return errors.New("Another tx is open.")
	}

	if err := b.txState().BeginTx(); err != nil {
		glog.Errorf("Cannot begin a transaction for %v: %v", b, err)
		return err
	}

	b.tx.Status = TxOpen
	b.tx.Generation = b.generation()
	b.tx.Seq++
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
	if err := b.txState().CommitTx(); err != nil {
		return err
	}

	b.tx.Status = TxCommitted
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

	b.tx.Ops = b.txState().Tx()
	if b.tx.IsEmpty() {
		b.tx.Seq--
		return b.doCommitTx()
	}

	colony := b.colony()
	if len(colony.Slaves) < b.app.CommitThreshold() {
		if err := b.tryToRecruitSlaves(); err != nil {
			glog.Errorf("Cannot create enough slaves to commit the transaction: %v",
				err)
			b.AbortTx()
			return fmt.Errorf("Not enough slaves to commit the transaction: %v", err)
		}
	}

	b.tx.Generation = colony.Generation

	retries := 5
	for {
		lives, deads := b.replicateTxOnAllSlaves(b.tx)
		if len(lives) >= b.app.CommitThreshold() {
			break
		}

		glog.Warningf("Replicated less than commit threshold %v", len(lives))

		if retries == 0 {
			// TODO(soheil): Should we really fail here?
			b.AbortTx()
			return fmt.Errorf("Can only replicate to %v slaves", len(lives))
		}
		retries--
		time.Sleep(5 * time.Millisecond)
		for _, s := range deads {
			glog.V(2).Infof("Trying to replace slave %v", s)
			b.handleSlaveFailure(s)
		}
		glog.V(2).Infof("Allocated new slaves will retry")
	}

	if err := b.doCommitTx(); err != nil {
		glog.Fatalf("Error in committing the transaction: %v", err)
	}

	b.txBuf = append(b.txBuf, b.tx)

	if err := b.sendCommitToAllSlaves(b.tx.Seq); err != nil {
		glog.Errorf("Cannot notify all salves about transaction: %v", err)
	}

	glog.V(2).Infof("Bee %v committed tx #%v", b, b.tx.Seq)
	return nil
}

func (b *localBee) AbortTx() error {
	if !b.tx.IsOpen() {
		return nil
	}

	glog.V(2).Infof("Bee %v aborts tx #%v", b, b.tx.Seq)
	b.tx.Reset()
	b.tx.Seq--
	return b.txState().AbortTx()
}

func (bee *localBee) Snooze(d time.Duration) {
	panic(d)
}
