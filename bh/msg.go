package bh

import "reflect"

// Message is a generic interface for messages emitted in the system. Messages
// are defined for each type.
type Msg interface {
	// Type of the message.
	Type() MsgType
	// Data stored in the message.
	Data() interface{}
	// Source of the message.
	From() BeeId
	// Destination of the message.
	To() BeeId
}

type MsgType string

type msg struct {
	MsgData interface{}
	MsgType MsgType
	MsgFrom BeeId
	MsgTo   BeeId
}

func (m *msg) NoReply() bool {
	return m.MsgFrom.isNil()
}

func (m *msg) isBroadCast() bool {
	return m.MsgTo.isNil()
}

func (m *msg) isUnicast() bool {
	return !m.MsgTo.isNil()
}

func (m *msg) Type() MsgType {
	return m.MsgType
}

func (m *msg) Data() interface{} {
	return m.MsgData
}

func (m *msg) To() BeeId {
	return m.MsgTo
}

func (m *msg) From() BeeId {
	return m.MsgFrom
}

func msgType(d interface{}) MsgType {
	return MsgType(reflect.TypeOf(d).String())
}

func newMsgFromData(data interface{}, from BeeId, to BeeId) *msg {
	return &msg{
		MsgType: msgType(data),
		MsgData: data,
		MsgFrom: from,
		MsgTo:   to,
	}
}
