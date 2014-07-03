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

type msg struct {
	MsgData interface{}
	MsgType MsgType
	From    RcvrId
	To      RcvrId
}

func (m *msg) noReply() bool {
	return m.From.isNil()
}

func (m *msg) isBroadCast() bool {
	return m.To.isNil()
}

func (m *msg) isUnicast() bool {
	return !m.To.isNil()
}

func (m *msg) Type() MsgType {
	return m.MsgType
}

func (m *msg) Data() interface{} {
	return m.MsgData
}

func msgType(d interface{}) MsgType {
	return MsgType(reflect.TypeOf(d).String())
}
