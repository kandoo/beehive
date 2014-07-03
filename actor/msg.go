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
	MsgData interface{}
	MsgType MsgType
}

type broadcastMsg struct {
	simpleMsg
	From RcvrId
}

type unicastMsg struct {
	broadcastMsg
	To RcvrId
}

func (m *simpleMsg) Type() MsgType {
	return m.MsgType
}

func (m *simpleMsg) Data() interface{} {
	return m.MsgData
}

func msgType(d interface{}) MsgType {
	return MsgType(reflect.TypeOf(d).String())
}
