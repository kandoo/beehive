package beehive

import (
	"time"

	"github.com/kandoo/beehive/state"
)

// MockMsg is a mock for Msg.
type MockMsg struct {
	MType string
	MData interface{}
	MTo   uint64
	MFrom uint64
}

func (m MockMsg) To() uint64 {
	return m.MTo
}

func (m MockMsg) From() uint64 {
	return m.MFrom
}

func (m MockMsg) Data() interface{} {
	return m.MData
}

func (m MockMsg) Type() string {
	return m.MType
}

func (m MockMsg) IsBroadCast() bool {
	return m.MTo == Nil
}

func (m MockMsg) IsUnicast() bool {
	return m.MTo != Nil
}

func (m MockMsg) NoReply() bool {
	return m.MFrom == Nil
}

// MockRcvContext is a mock for RcvContext.
type MockRcvContext struct {
	CtxHive  Hive
	CtxApp   string
	CtxDicts *state.InMem
	CtxID    uint64
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

func (m MockRcvContext) Emit(msgData interface{}) {}

func (m MockRcvContext) SendToCell(msgData interface{}, app string,
	cell CellKey) {
}

func (m MockRcvContext) SendToBee(msgData interface{}, to uint64) {}

func (m MockRcvContext) ReplyTo(msg Msg, replyData interface{}) error {
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
