package actor

import "reflect"

// Message is a generic interface for messages emitted in the system. Messages
// are defined for each type.
type Msg interface {
	// Type of the message.
	Type() MsgType
	// Data stored in the message.
	Data() interface{}
}

type MsgType string

type simpleMsg struct {
	stage   Stage
	data    interface{}
	msgType MsgType
}

type broadcastMsg struct {
	simpleMsg
	from ReceiverId
}

type unicastMsg struct {
	broadcastMsg
	to ReceiverId
}

func (m *simpleMsg) Type() MsgType {
	return m.msgType
}

func (m *simpleMsg) Data() interface{} {
	return m.data
}

func msgType(d interface{}) MsgType {
	return MsgType(reflect.TypeOf(d).String())
}
