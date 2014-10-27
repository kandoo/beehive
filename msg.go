package beehive

import (
	"encoding/gob"
	"fmt"
	"reflect"
)

// Message is a generic interface for messages emitted in the system. Messages
// are defined for each type.
type Msg interface {
	// Type of the data in this message.
	Type() string
	// Data stored in the message.
	Data() interface{}
	// From returns the ID of the sender of this message.
	From() uint64
	// To returns the ID of the receiver of this message.
	To() uint64

	// NoReply returns whether we can reply to the message.
	NoReply() bool
	// IsBroadCast returns whether the message is a broadcast.
	IsBroadCast() bool
	// IsUnicast returns whether the message is a unicast.
	IsUnicast() bool
}

// Typed is a message data with an explicit type.
type Typed interface {
	Type() string
}

type msg struct {
	MsgData interface{}
	MsgFrom uint64
	MsgTo   uint64
}

func (m msg) NoReply() bool {
	return m.MsgFrom == 0
}

func (m msg) IsBroadCast() bool {
	return m.MsgTo == 0
}

func (m msg) IsUnicast() bool {
	return m.MsgTo != 0
}

func (m msg) Type() string {
	return msgType(m.MsgData)
}

func (m msg) Data() interface{} {
	return m.MsgData
}

func (m msg) To() uint64 {
	return m.MsgTo
}

func (m msg) From() uint64 {
	return m.MsgFrom
}

func (m msg) String() string {
	return fmt.Sprintf("%v -> %v\t%v(%#v)", m.From(), m.To(), m.Type(), m.Data())
}

func msgType(d interface{}) string {
	if t, ok := d.(Typed); ok {
		return t.Type()
	}
	return reflect.TypeOf(d).String()
}

func newMsgFromData(data interface{}, from uint64, to uint64) *msg {
	return &msg{
		MsgData: data,
		MsgFrom: from,
		MsgTo:   to,
	}
}

type msgAndHandler struct {
	msg     *msg
	handler Handler
}

type Emitter interface {
	Emit(msgData interface{})
}

func init() {
	gob.Register(msg{})
}
