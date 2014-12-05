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
	return MsgType(m.MsgData)
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

// MsgType returns the message type of the data.
func MsgType(d interface{}) string {
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

type msgChannel struct {
	chin  chan msgAndHandler
	chout chan msgAndHandler
	buf   []msgAndHandler
	start int
	end   int
}

func newMsgChannel(bufSize int) *msgChannel {
	q := &msgChannel{
		chin:  make(chan msgAndHandler, bufSize),
		chout: make(chan msgAndHandler, bufSize),
		buf:   make([]msgAndHandler, bufSize),
	}
	go q.pipe()
	return q
}

func (q *msgChannel) pipe() {
	var chout chan msgAndHandler
	var first msgAndHandler
	dequed := false
	for {
		if dequed {
			chout = q.chout
		} else {
			chout = nil
		}
		select {
		case mh := <-q.chin:
			q.enque(mh)
			q.maybeReadMore()
			if dequed == false {
				first, dequed = q.deque()
			}
		case chout <- first:
			q.maybeWriteMore()
			first, dequed = q.deque()
		}
	}
}

func (q *msgChannel) maybeReadMore() {
	for l := len(q.chin); l > 0; l-- {
		select {
		case mh := <-q.chin:
			q.enque(mh)
		default:
			return
		}
	}
}

func (q *msgChannel) maybeWriteMore() {
	for l := q.len(); l > 0; l-- {
		select {
		case q.chout <- q.buf[q.start]:
			q.deque()
		default:
			return
		}
	}
}

func (q *msgChannel) in() chan<- msgAndHandler {
	return q.chin
}

func (q *msgChannel) out() <-chan msgAndHandler {
	return q.chout
}

func (q *msgChannel) empty() bool {
	return q.len() == 0
}

func (q *msgChannel) full() bool {
	return q.len() == len(q.buf)-1
}

func (q *msgChannel) enque(mh msgAndHandler) {
	if q.full() {
		q.maybeExpand()
	}

	q.buf[q.end] = mh
	q.end++
	if q.end >= len(q.buf) {
		q.end = 0
	}
}

func (q *msgChannel) deque() (msgAndHandler, bool) {
	if q.empty() {
		return msgAndHandler{}, false
	}

	mh := q.buf[q.start]
	q.start++
	if q.start >= len(q.buf) {
		q.start = 0
	}
	return mh, true
}

func (q *msgChannel) len() int {
	l := q.end - q.start
	if l >= 0 {
		return l
	}
	return len(q.buf) + l
}

func (q *msgChannel) maybeExpand() {
	if !q.full() {
		return
	}

	qlen := q.len()
	buf := make([]msgAndHandler, len(q.buf)*2)
	if q.start < q.end {
		copy(buf, q.buf[q.start:q.end])
	} else {
		l := len(q.buf) - q.start
		copy(buf, q.buf[q.start:])
		copy(buf[l:], q.buf[:q.end])
	}
	q.start = 0
	q.end = qlen
	q.buf = buf
}
