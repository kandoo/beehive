package beehive

import (
	"encoding/gob"
	"sync"

	bhgob "github.com/kandoo/beehive/gob"
)

var (
	// ErrSyncStopped returned when the sync handler is stopped before receiving
	// the response.
	ErrSyncStopped = bhgob.Error("sync: stopped")
	// ErrSyncNoSuchRequest returned when we cannot find the request for that
	// response.
	ErrSyncNoSuchRequest = bhgob.Error("sync: request not found")
	// ErrSyncDuplicateResponse returned when there is a duplicate repsonse to the
	// sync request.
	ErrSyncDuplicateResponse = bhgob.Error("sync: duplicate response")
)

// syncReq represents a generic sync request.
type syncReq struct {
	ID   uint64
	Data interface{} // Data of the request. Must be registered in gob.
}

// Type returns the type of this request. It is unique for each data type.
func (r syncReq) Type() string {
	if r.Data == nil {
		return "sync-req"
	}
	return "sync-req-" + MsgType(r.Data)
}

// syncRes represents a generic sync reponse.
type syncRes struct {
	ID   uint64      // ID is the request ID.
	Data interface{} // Data of the response. Must be registered in gob.
	Err  error       // Err is error, if any.
}

// Type returns the type of this response. It is unique for each data type.
func (r syncRes) Type() string {
	if r.Data == nil {
		return "syncRes"
	}
	return "syncRes-" + MsgType(r.Data)
}

type syncReqAndChan struct {
	req syncReq
	ch  chan syncRes
}

// syncDetached is a generic DetachedHandler for sync request processing, and
// also provides Handle, HandleFunc, and Process for the clients.
type syncDetached struct {
	sync.Mutex
	reqs map[uint64]chan syncRes

	reqch chan syncReqAndChan
	done  chan chan struct{}
}

// newSync creates a sync detached for the application that retrieves its
// requests from ch.
func newSync(a App, ch chan syncReqAndChan) *syncDetached {
	s := &syncDetached{
		reqs:  make(map[uint64]chan syncRes),
		reqch: ch,
		done:  make(chan chan struct{}),
	}
	a.Detached(s)
	return s
}

// Start is to implement DetachedHandler.
func (s *syncDetached) Start(ctx RcvContext) {
	for {
		select {
		case ch := <-s.done:
			s.drain()
			ch <- struct{}{}
		case rnc := <-s.reqch:
			s.enque(rnc.req.ID, rnc.ch)
			ctx.Emit(rnc.req)
		}
	}
}

// Stop is to implement DetachedHandler.
func (s *syncDetached) Stop(ctx RcvContext) {
	ack := make(chan struct{})
	s.done <- ack
	<-ack
}

// Rcv is to implement DetachedHandler.
func (s *syncDetached) Rcv(msg Msg, ctx RcvContext) error {
	res := msg.Data().(syncRes)
	ch, err := s.deque(res.ID)
	if err != nil {
		return err
	}
	ch <- res
	return nil
}

func (s *syncDetached) drain() {
	for id, ch := range s.reqs {
		s.Lock()
		ch <- syncRes{
			ID:  id,
			Err: ErrSyncStopped,
		}
		delete(s.reqs, id)
	}
	s.Unlock()
}

func (s *syncDetached) enque(id uint64, ch chan syncRes) {
	s.Lock()
	s.reqs[id] = ch
	s.Unlock()
}

func (s *syncDetached) deque(id uint64) (chan syncRes, error) {
	s.Lock()
	ch, ok := s.reqs[id]
	s.Unlock()
	if !ok {
		return nil, ErrSyncNoSuchRequest
	}
	delete(s.reqs, id)
	return ch, nil
}

type syncRcvContext struct {
	RcvContext
	id      uint64
	from    uint64
	replied bool
}

func (ctx *syncRcvContext) Reply(msg Msg, replyData interface{}) error {
	if msg.From() != ctx.from {
		return ctx.RcvContext.Reply(msg, replyData)
	}

	if ctx.replied {
		return ErrSyncDuplicateResponse
	}

	ctx.replied = true
	r := syncRes{
		ID:   ctx.id,
		Data: replyData,
	}
	return ctx.RcvContext.Reply(msg, r)
}

func (ctx *syncRcvContext) DeferReply(msg Msg) Repliable {
	ctx.replied = true
	return Repliable{
		From:   msg.From(),
		SyncID: ctx.id,
	}
}

type syncHandler struct {
	handler Handler
}

func (h syncHandler) Rcv(m Msg, ctx RcvContext) error {
	req := m.Data().(syncReq)
	sm := msg{
		MsgData: req.Data,
		MsgFrom: m.From(),
		MsgTo:   m.To(),
	}
	sc := syncRcvContext{
		RcvContext: ctx,
		id:         req.ID,
		from:       m.From(),
	}
	err := h.handler.Rcv(sm, &sc)
	if err != nil {
		ctx.AbortTx()
		r := syncRes{
			ID:  req.ID,
			Err: bhgob.Error(err.Error()),
		}
		ctx.Reply(m, r)
		return err
	}
	if !sc.replied {
		r := syncRes{
			ID: req.ID,
		}
		ctx.Reply(m, r)
	}
	return nil
}

func (h syncHandler) Map(m Msg, ctx MapContext) MappedCells {
	s := msg{
		MsgData: m.Data().(syncReq).Data,
		MsgFrom: m.From(),
		MsgTo:   m.To(),
	}
	return h.handler.Map(s, ctx)
}

func init() {
	gob.Register(syncReq{})
	gob.Register(syncRes{})
}

var _ DetachedHandler = &syncDetached{}
var _ Handler = syncHandler{}
