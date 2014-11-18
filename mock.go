package beehive

import (
	"errors"
	"time"

	"github.com/kandoo/beehive/state"
)

// MockMsg is a mock for Msg.
type MockMsg struct {
	MsgData interface{}
	MsgTo   uint64
	MsgFrom uint64
}

func (m MockMsg) To() uint64 {
	return m.MsgTo
}

func (m MockMsg) From() uint64 {
	return m.MsgFrom
}

func (m MockMsg) Data() interface{} {
	return m.MsgData
}

func (m MockMsg) Type() string {
	return msgType(m.MsgData)
}

func (m MockMsg) IsBroadCast() bool {
	return m.MsgTo == Nil
}

func (m MockMsg) IsUnicast() bool {
	return m.MsgTo != Nil
}

func (m MockMsg) NoReply() bool {
	return m.MsgFrom == Nil
}

// MockRcvContext is a mock for RcvContext.
type MockRcvContext struct {
	CtxHive  Hive
	CtxApp   string
	CtxDicts *state.InMem
	CtxID    uint64
	CtxMsgs  []Msg
	// TODO(soheil): add message handling methods.
}

func (m MockRcvContext) Hive() Hive {
	return m.CtxHive
}

func (m MockRcvContext) App() string {
	return m.CtxApp
}

func (m *MockRcvContext) Dict(name string) state.Dict {
	if m.CtxDicts == nil {
		m.CtxDicts = state.NewInMem()
	}
	return m.CtxDicts.Dict(name)
}

func (m MockRcvContext) ID() uint64 {
	return m.CtxID
}

func (m *MockRcvContext) Emit(msgData interface{}) {
	msg := MockMsg{
		MsgData: msgData,
		MsgFrom: m.ID(),
	}
	m.CtxMsgs = append(m.CtxMsgs, msg)
}

func (m MockRcvContext) SendToCell(msgData interface{}, app string,
	cell CellKey) {
}

func (m *MockRcvContext) SendToBee(msgData interface{}, to uint64) {
	msg := MockMsg{
		MsgData: msgData,
		MsgFrom: m.ID(),
		MsgTo:   to,
	}
	m.CtxMsgs = append(m.CtxMsgs, msg)
}

func (m *MockRcvContext) ReplyTo(msg Msg, replyData interface{}) error {
	if msg.NoReply() {
		return errors.New("cannot reply")
	}
	m.SendToBee(replyData, msg.To())
	return nil
}

func (m MockRcvContext) StartDetached(h DetachedHandler) uint64 {
	return 0
}

func (m MockRcvContext) StartDetachedFunc(start StartFunc, stop StopFunc,
	rcv RcvFunc) uint64 {
	return 0
}

func (m MockRcvContext) LockCells(keys []CellKey) error {
	return nil
}

func (m MockRcvContext) Snooze(d time.Duration) {}

func (m MockRcvContext) BeeLocal() interface{} {
	return nil
}

func (m MockRcvContext) SetBeeLocal(d interface{}) {}

func (m MockRcvContext) BeginTx() error {
	return nil
}

func (m MockRcvContext) CommitTx() error {
	return nil
}

func (m MockRcvContext) AbortTx() error {
	return nil
}
