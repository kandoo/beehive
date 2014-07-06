package actor

import (
	"errors"

	"github.com/golang/glog"
)

type Context interface {
	State() State
}

type RecvContext interface {
	Context
	Emit(msgData interface{})
	SendTo(msgData interface{}, to ActorName, dk DictionaryKey)
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

// Emits a message. Note that m should be your data not an instance of Msg.
func (ctx *recvContext) Emit(msgData interface{}) {
	msg := msg{
		MsgData: msgData,
		MsgType: msgType(msgData),
		From:    ctx.rcvr.id(),
	}
	ctx.stage.emitMsg(&msg)
}

func (ctx *recvContext) SendTo(msgData interface{}, to ActorName,
	dk DictionaryKey) {

	msg := msg{
		MsgData: msgData,
		MsgType: msgType(msgData),
		From:    ctx.rcvr.id(),
	}

	ctx.stage.emitMsg(&msg)

	// TODO(soheil): Implement send to.
	glog.Fatal("Sendto is not implemented.")
}

// Reply to thatMsg with the provided replyData.
func (ctx *recvContext) ReplyTo(thatMsg Msg, replyData interface{}) error {
	m := thatMsg.(*msg)
	if m.noReply() {
		return errors.New("Cannot reply to this message.")
	}

	ctx.stage.emitMsg(newMsgFromData(replyData, ctx.rcvr.id(), m.From))
	return nil
}
