package bh

import (
	"errors"

	"github.com/golang/glog"
)

type MapContext interface {
	Hive() Hive
	State() State
	Dict(n DictionaryName) Dictionary
}

type RcvContext interface {
	MapContext

	BeeId() BeeId

	Emit(msgData interface{})
	SendToDictKey(msgData interface{}, to AppName, dk DictionaryKey)
	SendToBee(msgData interface{}, to BeeId)
	ReplyTo(msg Msg, replyData interface{}) error

	StartDetached(h DetachedHandler) BeeId
	StartDetachedFunc(start Start, stop Stop, rcv Rcv) BeeId

	Lock(ms MapSet) error

	BeeLocal() interface{}
	SetBeeLocal(d interface{})
}

type mapContext struct {
	state State
	hive  *hive
	app   *app
}

type rcvContext struct {
	mapContext
	bee   bee
	local interface{}
}

// Creates a new receiver context.
func (mc mapContext) newRcvContext() rcvContext {
	rc := rcvContext{mapContext: mc}
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

func (ctx *mapContext) Dict(n DictionaryName) Dictionary {
	return ctx.State().Dict(n)
}

func (ctx *mapContext) Hive() Hive {
	return ctx.hive
}

// Emits a message. Note that m should be your data not an instance of Msg.
func (ctx *rcvContext) Emit(msgData interface{}) {
	ctx.hive.emitMsg(newMsgFromData(msgData, ctx.bee.id(), BeeId{}))
}

func (ctx *rcvContext) SendToDictKey(msgData interface{}, to AppName,
	dk DictionaryKey) {

	// TODO(soheil): Implement send to.
	msg := newMsgFromData(msgData, ctx.bee.id(), BeeId{})
	ctx.hive.emitMsg(msg)

	glog.Fatal("Sendto is not implemented.")
}

func (ctx *rcvContext) SendToBee(msgData interface{}, to BeeId) {
	ctx.hive.emitMsg(newMsgFromData(msgData, ctx.bee.id(), to))
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

func (ctx *rcvContext) Lock(ms MapSet) error {
	resCh := make(chan CmdResult)
	ctx.app.qee.ctrlCh <- LocalCmd{
		CmdType: lockMapSetCmd,
		CmdData: lockMapSetData{
			MapSet: ms,
			BeeId:  ctx.bee.id(),
		},
		ResCh: resCh,
	}
	res := <-resCh
	return res.Err
}

func (ctx *rcvContext) SetBeeLocal(d interface{}) {
	ctx.local = d
}

func (ctx *rcvContext) BeeLocal() interface{} {
	return ctx.local
}

func (ctx *rcvContext) StartDetached(h DetachedHandler) BeeId {
	resCh := make(chan CmdResult)
	cmd := LocalCmd{
		CmdType: startDetachedCmd,
		CmdData: h,
		ResCh:   resCh,
	}

	switch b := ctx.bee.(type) {
	case *localBee:
		b.qee.ctrlCh <- cmd
	case *detachedBee:
		b.qee.ctrlCh <- cmd
	}

	return (<-resCh).Data.(BeeId)
}

func (ctx *rcvContext) StartDetachedFunc(start Start, stop Stop, rcv Rcv) BeeId {
	return ctx.StartDetached(&funcDetached{start, stop, rcv})
}

func (ctx *rcvContext) BeeId() BeeId {
	return ctx.bee.id()
}
