package composition

import (
	"time"

	bh "github.com/kandoo/beehive"
	"github.com/kandoo/beehive/state"
)

func newMockContext() *mockContext {
	ctx := &mockContext{
		Transactional: state.NewTransactional(state.NewInMem()),
	}
	return ctx
}

type mockContext struct {
	*state.Transactional
	txAborted bool
}

func (c mockContext) Hive() bh.Hive {
	return nil
}

func (c mockContext) App() string {
	return ""
}

func (c mockContext) LocalMappedCells() bh.MappedCells {
	return bh.MappedCells{{"D", "K"}}
}

func (c mockContext) ID() uint64 {
	return 0
}

func (c mockContext) Emit(msgData interface{})                 {}
func (c mockContext) SendToBee(msgData interface{}, to uint64) {}
func (c mockContext) SendToCell(msgData interface{}, to string,
	dk bh.CellKey) {
}
func (c mockContext) ReplyTo(msg bh.Msg, replyData interface{}) error {
	return nil
}

func (c mockContext) StartDetached(h bh.DetachedHandler) uint64 { return 0 }
func (c mockContext) StartDetachedFunc(start bh.StartFunc, stop bh.StopFunc,
	rcv bh.RcvFunc) uint64 {
	return 0
}
func (c mockContext) LockCells(keys []bh.CellKey) error { return nil }
func (c mockContext) Snooze(d time.Duration)            {}
func (c mockContext) BeeLocal() interface{}             { return nil }
func (c mockContext) SetBeeLocal(d interface{})         {}

func (c mockContext) CommitTx() error {
	c.txAborted = false
	return c.Transactional.CommitTx()
}

func (c mockContext) AbortTx() error {
	c.txAborted = true
	return c.Transactional.AbortTx()
}

func (c mockContext) DeferReply(msg bh.Msg) bh.Repliable {
	return bh.Repliable{}
}
