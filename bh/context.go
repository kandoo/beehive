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
	Emit(msgData interface{})
	SendToDictKey(msgData interface{}, to AppName, dk DictionaryKey)
	SendToBee(msgData interface{}, to BeeId)
	ReplyTo(msg Msg, replyData interface{}) error
	Lock(ms MapSet) error
}

type mapContext struct {
	state State
	hive  *hive
	app   *app
}

type rcvContext struct {
	mapContext
	bee bee
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
	resCh := make(chan asyncResult)
	ctx.app.qee.ctrlCh <- routineCmd{
		lockMapSetCmd,
		lockMapSetData{
			MapSet: ms,
			BeeId:  ctx.bee.id(),
		},
		resCh,
	}
	res := <-resCh
	return res.err
}
