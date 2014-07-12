package actor

import (
	"errors"

	"github.com/golang/glog"
)

type Context interface {
	Stage() Stage
	State() State
	Dict(n DictionaryName) Dictionary
}

type RecvContext interface {
	Context
	Emit(msgData interface{})
	SendToDictKey(msgData interface{}, to ActorName, dk DictionaryKey)
	SendToRcvr(msgData interface{}, to RcvrId)
	ReplyTo(msg Msg, replyData interface{}) error
}

type context struct {
	state State
	stage *stage
	actor *actor
}

type recvContext struct {
	context
	rcvr receiver
}

func (ctx *context) State() State {
	if ctx.state == nil {
		ctx.state = newState(string(ctx.actor.Name()))
	}

	return ctx.state
}

func (ctx *context) Dict(n DictionaryName) Dictionary {
	return ctx.State().Dict(n)
}

func (ctx *context) Stage() Stage {
	return ctx.stage
}

// Emits a message. Note that m should be your data not an instance of Msg.
func (ctx *recvContext) Emit(msgData interface{}) {
	ctx.stage.emitMsg(newMsgFromData(msgData, ctx.rcvr.id(), RcvrId{}))
}

func (ctx *recvContext) SendToDictKey(msgData interface{}, to ActorName,
	dk DictionaryKey) {

	// TODO(soheil): Implement send to.
	msg := newMsgFromData(msgData, ctx.rcvr.id(), RcvrId{})
	ctx.stage.emitMsg(msg)

	glog.Fatal("Sendto is not implemented.")
}

func (ctx *recvContext) SendToRcvr(msgData interface{}, to RcvrId) {
	ctx.stage.emitMsg(newMsgFromData(msgData, ctx.rcvr.id(), to))
}

// Reply to thatMsg with the provided replyData.
func (ctx *recvContext) ReplyTo(thatMsg Msg, replyData interface{}) error {
	m := thatMsg.(*msg)
	if m.noReply() {
		return errors.New("Cannot reply to this message.")
	}

	ctx.SendToRcvr(replyData, m.From())
	return nil
}
