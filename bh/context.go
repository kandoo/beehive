package bh

import (
	"errors"

	"github.com/golang/glog"
)

type Context interface {
	Hive() Hive
	State() State
	Dict(n DictionaryName) Dictionary
}

type RecvContext interface {
	Context
	Emit(msgData interface{})
	SendToDictKey(msgData interface{}, to AppName, dk DictionaryKey)
	SendToRcvr(msgData interface{}, to RcvrId)
	ReplyTo(msg Msg, replyData interface{}) error
}

type context struct {
	state State
	hive  *hive
	app   *app
}

type recvContext struct {
	context
	rcvr receiver
}

func (ctx *context) State() State {
	if ctx.state == nil {
		ctx.state = newState(string(ctx.app.Name()))
	}

	return ctx.state
}

func (ctx *context) Dict(n DictionaryName) Dictionary {
	return ctx.State().Dict(n)
}

func (ctx *context) Hive() Hive {
	return ctx.hive
}

// Emits a message. Note that m should be your data not an instance of Msg.
func (ctx *recvContext) Emit(msgData interface{}) {
	ctx.hive.emitMsg(newMsgFromData(msgData, ctx.rcvr.id(), RcvrId{}))
}

func (ctx *recvContext) SendToDictKey(msgData interface{}, to AppName,
	dk DictionaryKey) {

	// TODO(soheil): Implement send to.
	msg := newMsgFromData(msgData, ctx.rcvr.id(), RcvrId{})
	ctx.hive.emitMsg(msg)

	glog.Fatal("Sendto is not implemented.")
}

func (ctx *recvContext) SendToRcvr(msgData interface{}, to RcvrId) {
	ctx.hive.emitMsg(newMsgFromData(msgData, ctx.rcvr.id(), to))
}

// Reply to thatMsg with the provided replyData.
func (ctx *recvContext) ReplyTo(thatMsg Msg, replyData interface{}) error {
	m := thatMsg.(*msg)
	if m.NoReply() {
		return errors.New("Cannot reply to this message.")
	}

	ctx.SendToRcvr(replyData, m.From())
	return nil
}
