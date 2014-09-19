package bh

import (
	"fmt"
	"reflect"
)

// Message is a generic interface for messages emitted in the system. Messages
// are defined for each type.
type Msg interface {
	// Type of the message.
	Type() MsgType
	// Data stored in the message.
	Data() interface{}
	// Source of the message.
	From() BeeID
	// Destination of the message.
	To() BeeID
}

type MsgType string

type msg struct {
	MsgData interface{}
	MsgType MsgType
	MsgFrom BeeID
	MsgTo   BeeID
}

func (m *msg) NoReply() bool {
	return m.MsgFrom.IsNil()
}

func (m *msg) isBroadCast() bool {
	return m.MsgTo.IsNil()
}

func (m *msg) isUnicast() bool {
	return !m.MsgTo.IsNil()
}

func (m msg) Type() MsgType {
	return m.MsgType
}

func (m msg) Data() interface{} {
	return m.MsgData
}

func (m msg) To() BeeID {
	return m.MsgTo
}

func (m msg) From() BeeID {
	return m.MsgFrom
}

func (m msg) String() string {
	return fmt.Sprintf("%v -> %v\t%v(%#v)", m.MsgFrom, m.MsgTo, m.MsgType,
		m.MsgData)
}

func msgType(d interface{}) MsgType {
	return MsgType(reflect.TypeOf(d).String())
}

func newMsgFromData(data interface{}, from BeeID, to BeeID) *msg {
	return &msg{
		MsgType: msgType(data),
		MsgData: data,
		MsgFrom: from,
		MsgTo:   to,
	}
}

type msgAndHandler struct {
	msg     *msg
	handler Handler
}
