package actor

import "github.com/golang/glog"

type Context interface {
	State() State
}

type RecvContext interface {
	Context
	Emit(msgData interface{})
	SendTo(msgData interface{}, to ActorName, dk DictionaryKey)
	ReplyTo(msg Msg, replyData interface{})
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
	msg := broadcastMsg{
		simpleMsg: simpleMsg{
			stage:   ctx.stage,
			MsgData: msgData,
			MsgType: msgType(msgData),
		},
		From: ctx.rcvr.id(),
	}

	ctx.stage.EmitMsg(&msg)
}

func (ctx *recvContext) SendTo(msgData interface{}, to ActorName,
	dk DictionaryKey) {

	msg := unicastMsg{
		broadcastMsg: broadcastMsg{
			simpleMsg: simpleMsg{
				stage:   ctx.stage,
				MsgData: msgData,
				MsgType: msgType(msgData),
			},
			From: ctx.rcvr.id(),
		},
	}

	ctx.stage.EmitMsg(&msg)

	// TODO(soheil): Implement send to.
	glog.Fatal("Sendto is not implemented.")
}

func (ctx *recvContext) ReplyTo(msg Msg, replyData interface{}) {
	// TODO(soheil): Implement reply to.
	glog.Fatal("RelpyTo is not implemented.")
}
