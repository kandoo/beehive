package bh

import (
	"errors"

	"github.com/golang/glog"
)

// MapContext is passed to the map functions of message handlers. It provides
// all the platform-level functions required to implement the map function.
type MapContext interface {
	// Hive returns the Hive of this context.
	Hive() Hive
	// State returns the state of the bee/queen bee.
	State() State
	// Dict is a helper function that returns the specific dict within the state.
	Dict(n DictName) Dict
}

// RcvContext is passed to the rcv functions of message handlers. It provides
// all the platform-level functions required to implement the rcv function.
type RcvContext interface {
	MapContext

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
	StartDetachedFunc(start Start, stop Stop, rcv Rcv) BeeID

	Lock(ms MappedCells) error

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

type rcvContext struct {
	mapContext
	bee   bee
	local interface{}
	tx    Tx
}

// Creates a new receiver context.
func (ctx mapContext) newRcvContext() rcvContext {
	rc := rcvContext{mapContext: ctx}
	// We need to reset the state for the new bees.
	rc.state = nil
	return rc
}

func (ctx *mapContext) State() State {
	if ctx.state == nil {
		ctx.state = newState(ctx.app)
	}

	return ctx.state
}

func (ctx *mapContext) Dict(n DictName) Dict {
	return ctx.State().Dict(n)
}

func (ctx *mapContext) Hive() Hive {
	return ctx.hive
}

// Emits a message. Note that m should be your data not an instance of Msg.
func (ctx *rcvContext) Emit(msgData interface{}) {
	ctx.bufferOrEmit(newMsgFromData(msgData, ctx.bee.id(), BeeID{}))
}

func (ctx *rcvContext) doEmit(msg *msg) {
	ctx.hive.emitMsg(msg)
}

func (ctx *rcvContext) bufferOrEmit(msg *msg) {
	if !ctx.tx.IsOpen() {
		ctx.doEmit(msg)
		return
	}

	glog.V(2).Infof("Buffers msg %+v in tx %d", msg, ctx.tx.Seq)
	ctx.tx.AddMsg(msg)
}

func (ctx *rcvContext) SendToCellKey(msgData interface{}, to AppName,
	dk CellKey) {
	// TODO(soheil): Implement send to.
	glog.Fatal("Sendto is not implemented.")

	msg := newMsgFromData(msgData, ctx.bee.id(), BeeID{})
	ctx.bufferOrEmit(msg)
}

func (ctx *rcvContext) SendToBee(msgData interface{}, to BeeID) {
	ctx.bufferOrEmit(newMsgFromData(msgData, ctx.bee.id(), to))
}

// Reply to thatMsg with the provided replyData.
func (ctx *rcvContext) ReplyTo(thatMsg Msg, replyData interface{}) error {
	m := thatMsg.(*msg)
	if m.NoReply() {
		return errors.New("Cannot reply to this message.")
	}

	ctx.SendToBee(replyData, m.From())
	return nil
}

func (ctx *rcvContext) Lock(ms MappedCells) error {
	resCh := make(chan CmdResult)
	d := lockMappedCellsData{
		MappedCells: ms,
		Colony:      ctx.bee.colonyUnsafe(),
	}
	ctx.app.qee.ctrlCh <- NewLocalCmd(lockMappedCellsCmd, d, BeeID{}, resCh)
	res := <-resCh
	return res.Err
}

func (ctx *rcvContext) SetBeeLocal(d interface{}) {
	ctx.local = d
}

func (ctx *rcvContext) BeeLocal() interface{} {
	return ctx.local
}

func (ctx *rcvContext) StartDetached(h DetachedHandler) BeeID {
	resCh := make(chan CmdResult)
	cmd := NewLocalCmd(startDetachedCmd, h, BeeID{}, resCh)

	switch b := ctx.bee.(type) {
	case *localBee:
		b.qee.ctrlCh <- cmd
	case *detachedBee:
		b.qee.ctrlCh <- cmd
	}

	return (<-resCh).Data.(BeeID)
}

func (ctx *rcvContext) StartDetachedFunc(start Start, stop Stop, rcv Rcv) BeeID {
	return ctx.StartDetached(&funcDetached{start, stop, rcv})
}

func (ctx *rcvContext) BeeID() BeeID {
	return ctx.bee.id()
}

func (ctx *rcvContext) BeginTx() error {
	if ctx.tx.IsOpen() {
		return errors.New("Another tx is open.")
	}

	if err := ctx.bee.state().BeginTx(); err != nil {
		return err
	}

	ctx.tx.Status = TxOpen
	ctx.tx.Seq++
	return nil
}

func (ctx *rcvContext) emitTxMsgs() {
	if ctx.tx.Msgs == nil {
		return
	}

	for _, m := range ctx.tx.Msgs {
		ctx.doEmit(m.(*msg))
	}
}

func (ctx *rcvContext) doCommitTx() error {
	defer ctx.tx.Reset()
	ctx.emitTxMsgs()
	return ctx.bee.state().CommitTx()
}

func (ctx *rcvContext) CommitTx() error {
	if !ctx.tx.IsOpen() {
		return nil
	}

	if ctx.app.ReplicationFactor() < 2 {
		ctx.doCommitTx()
	}

	ctx.tx.Ops = ctx.state.Tx()

	if err := ctx.bee.replicateTx(&ctx.tx); err != nil {
		glog.Errorf("Error in replicating the transaction: %v", err)
		ctx.AbortTx()
		return err
	}

	if err := ctx.doCommitTx(); err != nil {
		glog.Fatalf("Error in committing the transaction: %v", err)
	}

	if err := ctx.bee.notifyCommitTx(ctx.tx.Seq); err != nil {
		glog.Errorf("Cannot notify all salves about transaction: %v", err)
	}

	return nil
}

func (ctx *rcvContext) AbortTx() error {
	if !ctx.tx.IsOpen() {
		return nil
	}

	ctx.tx.Reset()
	return ctx.bee.state().AbortTx()
}
