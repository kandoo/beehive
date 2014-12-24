package beehive

import (
	"bytes"
	"encoding/gob"
	"errors"
	"sync"
	"time"

	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/coreos/etcd/raft/raftpb"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/golang/glog"
	"github.com/kandoo/beehive/raft"
)

var (
	errStreamerBcastMsg  = errors.New("streamer: cannot stream a b-cast message")
	errStreamerStopped   = errors.New("streamer: stopped")
	errStreamerCancelled = errors.New("streamer: ")
)

type streamer interface {
	sendMsg(ms []msg) error
	sendCmd(c cmd, to uint64) (interface{}, error)
	sendRaft(ms []raftpb.Message) error
	sendBeeRaft(ms []raftpb.Message) error
	stop()
	// TODO(soheil): do we need start()?
}

type loadBalancer struct {
	sync.RWMutex

	h *hive

	bph  int                    // batchers per host.
	htob map[uint64]*rrBatchers // batchers for the hive.
	btob map[uint64]*batcher    // batcher for the bee.

	done chan struct{}
}

var _ streamer = &loadBalancer{}

func newLoadBalancer(h *hive, bph int) *loadBalancer {
	return &loadBalancer{
		h:    h,
		bph:  bph,
		htob: make(map[uint64]*rrBatchers),
		btob: make(map[uint64]*batcher),
		done: make(chan struct{}),
	}
}

func (lb *loadBalancer) hiveRRBatchersUnsafe(h uint64) *rrBatchers {
	rrb, ok := lb.htob[h]
	if !ok {
		rrb = newRRBatchers(lb.h, h)
		lb.htob[h] = rrb
	}
	return rrb
}

func (lb *loadBalancer) beeBatcher(b uint64) (*batcher, error) {
	lb.Lock()
	defer lb.Unlock()

	btchr, ok := lb.btob[b]
	if ok {
		return btchr, nil
	}

	bi, err := lb.h.bee(b)
	if err != nil {
		return nil, err
	}

	rrb := lb.hiveRRBatchersUnsafe(bi.Hive)
	if rrb.len() < lb.bph {
		if err := rrb.add(); err != nil {
			return nil, err
		}
	}

	btchr = rrb.batcher()
	lb.btob[b] = btchr
	return btchr, nil
}

func (lb *loadBalancer) hiveBatcher(h uint64) (*batcher, error) {
	lb.Lock()
	defer lb.Unlock()

	rrb, ok := lb.htob[h]
	if !ok {
		rrb = newRRBatchers(lb.h, h)
		lb.htob[h] = rrb
	}
	if rrb.len() < 1 {
		if err := rrb.add(); err != nil {
			return nil, err
		}
	}
	return rrb.bts[0], nil
}

func (lb *loadBalancer) sendMsg(ms []msg) error {
	if lb.stopped() {
		return errStreamerStopped
	}

	for _, m := range ms {
		if m.To() == Nil {
			glog.Error("loadbalancer cannot send b-case message")
			continue
		}
		btchr, err := lb.beeBatcher(m.To())
		if err != nil {
			glog.Errorf("cannot create batcher for bee %v: %v", m.To(), err)
			continue
		}
		btchr.enqueMsg(m)
	}
	return nil
}

func (lb *loadBalancer) sendCmd(c cmd, to uint64) (interface{}, error) {
	if lb.stopped() {
		return nil, errStreamerStopped
	}

	var btchr *batcher
	var err error
	if c.To == Nil {
		btchr, err = lb.hiveBatcher(to)
	} else {
		btchr, err = lb.beeBatcher(c.To)
	}
	if err != nil {
		return nil, err
	}
	return btchr.sendCmd(c, to)
}

func (lb *loadBalancer) sendRaft(ms []raftpb.Message) error {
	if lb.stopped() {
		return errStreamerStopped
	}

	for _, m := range ms {
		btchr, err := lb.hiveBatcher(m.To)
		if err != nil {
			return err
		}
		if err = btchr.enqueRaft(m); err != nil {
			return err
		}
	}
	return nil
}

func (lb *loadBalancer) sendBeeRaft(ms []raftpb.Message) error {
	if lb.stopped() {
		return errStreamerStopped
	}

	for _, m := range ms {
		btchr, err := lb.beeBatcher(m.To)
		if err != nil {
			return err
		}
		if err = btchr.enqueBeeRaft(m); err != nil {
			return err
		}
	}
	return nil
}

func (lb *loadBalancer) stop() {
	close(lb.done)
}

type rrBatchers struct {
	h  *hive
	to uint64

	i   int
	bts []*batcher
}

func newRRBatchers(h *hive, to uint64) *rrBatchers {
	return &rrBatchers{h: h, to: to}
}

func (rr *rrBatchers) add() error {
	b, err := newBatcher(rr.h, rr.to)
	if err != nil {
		return err
	}
	rr.bts = append(rr.bts, b)
	return nil
}

func (rr *rrBatchers) batcher() *batcher {
	rr.i = (rr.i + 1) % rr.len()
	return rr.bts[rr.i]
}

func (rr *rrBatchers) len() int {
	return len(rr.bts)
}

func (lb *loadBalancer) stopped() bool {
	select {
	case <-lb.done:
		return true
	default:
		return false
	}
}

const (
	batcherMsgIndex = iota
	batcherCmdIndex
	batcherRaftIndex
	batcherBeeRaftIndex
)

type batcher struct {
	h *hive

	batchTick time.Duration
	weights   [4]int

	msgs   chan msg
	cmds   chan cmdAndChannel
	rafts  chan raftpb.Message
	bRafts chan raftpb.Message

	done chan struct{}

	prx *proxy
}

func newBatcher(h *hive, to uint64) (*batcher, error) {
	// TODO(soheil): should we use inifinite channels here?
	prx, err := h.newProxyToHive(to)
	if err != nil {
		return nil, err
	}
	b := &batcher{
		h:         h,
		batchTick: h.config.BatcherTimeout,
		weights:   [4]int{1, 5, 10, 10},
		msgs:      make(chan msg, h.config.DataChBufSize),
		cmds:      make(chan cmdAndChannel, h.config.CmdChBufSize),
		rafts:     make(chan raftpb.Message, h.config.DataChBufSize),
		bRafts:    make(chan raftpb.Message, h.config.DataChBufSize),
		done:      make(chan struct{}),
		prx:       prx,
	}
	go b.start()
	return b, nil
}

func (b *batcher) batchRaft(wg *sync.WaitGroup) {
	defer wg.Done()

	var raftBuf bytes.Buffer
	raftEnc := raft.NewEncoder(&raftBuf)

	raftd := b.batchTick * time.Duration(b.weights[batcherRaftIndex])
	var tch <-chan time.Time

	for {
		reset := false
		select {
		case r := <-b.rafts:
			if err := raftEnc.Encode(r); err != nil {
				glog.Errorf("cannot encode raft message: %v", err)
				reset = true
			}
			if tch == nil {
				tch = time.After(raftd)
			}

		case <-tch:
			if raftBuf.Len() == 0 {
				tch = nil
				continue
			}

			err := b.prx.sendRaftNew(&raftBuf)
			if err != nil {
				glog.Errorf("error in sending raft to %v: %v", b.prx.to, err)
			}
			reset = true

		case <-b.done:
			return
		}

		if reset {
			tch = nil
			raftBuf.Reset()
			raftEnc = raft.NewEncoder(&raftBuf)
		}
	}
}

func (b *batcher) batchBeeRaft(wg *sync.WaitGroup) {
	defer wg.Done()

	var bRaftBuf bytes.Buffer
	bRaftEnc := raft.NewEncoder(&bRaftBuf)

	braftd := b.batchTick * time.Duration(b.weights[batcherBeeRaftIndex])
	var tch <-chan time.Time

	for {
		reset := false
		select {
		case r := <-b.bRafts:
			if err := bRaftEnc.Encode(r); err != nil {
				glog.Errorf("cannot encode raft message: %v", err)
				reset = true
			}

			if tch == nil {
				tch = time.After(braftd)
			}

		case <-tch:
			if bRaftBuf.Len() == 0 {
				tch = nil
				continue
			}

			err := b.prx.sendBeeRaftNew(&bRaftBuf)
			if err != nil {
				glog.Errorf("error in sending bee raft to %v: %v", b.prx.to, err)
			}
			reset = true

		case <-b.done:
			return
		}

		if reset {
			tch = nil
			bRaftBuf.Reset()
			bRaftEnc = raft.NewEncoder(&bRaftBuf)
		}
	}
}

func (b *batcher) batchMsg(wg *sync.WaitGroup) {
	defer wg.Done()

	var msgBuf bytes.Buffer
	msgEnc := gob.NewEncoder(&msgBuf)

	msgd := b.batchTick * time.Duration(b.weights[batcherMsgIndex])
	var tch <-chan time.Time

	for {
		reset := false
		select {
		case m := <-b.msgs:
			if err := msgEnc.Encode(m); err != nil {
				glog.Errorf("cannot encode message: %v", err)
				reset = true
			}

			if tch == nil {
				tch = time.After(msgd)
			}

		case <-tch:
			if msgBuf.Len() == 0 {
				tch = nil
				continue
			}

			err := b.prx.sendMsgNew(&msgBuf)
			if err != nil {
				glog.Errorf("error in sending messages %v: %v", b.prx.to, err)
			}
			reset = true

		case <-b.done:
			return
		}

		if reset {
			tch = nil
			msgBuf.Reset()
			msgEnc = gob.NewEncoder(&msgBuf)
		}
	}
}

func (b *batcher) batchCmd(wg *sync.WaitGroup) {
	defer wg.Done()

	var cmds []cmdAndChannel
	var cmdBuf bytes.Buffer
	cmdEnc := gob.NewEncoder(&cmdBuf)

	cmdd := b.batchTick * time.Duration(b.weights[batcherCmdIndex])
	var tch <-chan time.Time

	for {
		reset := false
		select {
		case c := <-b.cmds:
			cmds = append(cmds, c)
			if err := cmdEnc.Encode(c.cmd); err != nil {
				glog.Errorf("cannot encode command: %v", err)
				cmdBuf.Reset()
				cmdEnc = gob.NewEncoder(&cmdBuf)
				for _, cc := range cmds {
					cc.ch <- cmdResult{Err: err}
				}
				cmds = cmds[:0]
				reset = true
			}

			if tch == nil {
				tch = time.After(cmdd)
			}

		case <-tch:
			if cmdBuf.Len() == 0 {
				tch = nil
				continue
			}

			res, err := b.prx.sendCmdNew(&cmdBuf)
			if err != nil {
				glog.Errorf("error in sending cmd to %v: %v", b.prx.to, err)
			} else {
				dec := gob.NewDecoder(res.Body)
				var i int
				for i = range cmds {
					var cr cmdResult
					if err := dec.Decode(&cr); err != nil {
						glog.Errorf("error in decoding results from %v: %v", b.prx.to, err)
						break
					}
					if cmds[i].ch == nil {
						continue
					}
					cmds[i].ch <- cr
				}
				for ; i < len(cmds); i++ {
					if cmds[i].ch == nil {
						continue
					}
					cmds[i].ch <- cmdResult{Err: errStreamerCancelled}
				}
			}
			maybeCloseResponse(res)
			reset = true

		case <-b.done:
			return
		}

		if reset {
			tch = nil
			cmdBuf.Reset()
			cmdEnc = gob.NewEncoder(&cmdBuf)
			cmds = cmds[:0]
		}
	}
}

func (b *batcher) start() {

	var wg sync.WaitGroup
	wg.Add(4)

	go b.batchRaft(&wg)
	go b.batchBeeRaft(&wg)
	go b.batchMsg(&wg)
	go b.batchCmd(&wg)

	wg.Wait()
}

func (b *batcher) sendMsg(ms []msg) error {
	if b.stopped() {
		return errStreamerStopped
	}

	for _, m := range ms {
		b.msgs <- m
	}
	return nil
}

func (b *batcher) enqueMsg(m msg) error {
	if b.stopped() {
		return errStreamerStopped
	}

	b.msgs <- m
	return nil
}

func (b *batcher) sendCmd(c cmd, to uint64) (interface{}, error) {
	if b.stopped() {
		return nil, errStreamerStopped
	}

	ch := make(chan cmdResult, 1)
	cc := cmdAndChannel{
		cmd: c,
		ch:  ch,
	}
	b.cmds <- cc
	return (<-ch).get()
}

func (b *batcher) sendRaft(ms []raftpb.Message) error {
	if b.stopped() {
		return errStreamerStopped
	}

	for _, m := range ms {
		b.rafts <- m
	}
	return nil
}

func (b *batcher) enqueRaft(m raftpb.Message) error {
	if b.stopped() {
		return errStreamerStopped
	}

	b.rafts <- m
	return nil
}

func (b *batcher) sendBeeRaft(ms []raftpb.Message) error {
	if b.stopped() {
		return errStreamerStopped
	}

	for _, m := range ms {
		b.bRafts <- m
	}
	return nil
}

func (b *batcher) enqueBeeRaft(m raftpb.Message) error {
	if b.stopped() {
		return errStreamerStopped
	}

	b.bRafts <- m
	return nil
}

func (b *batcher) stop() {
	close(b.done)
}

func (b *batcher) stopped() bool {
	select {
	case <-b.done:
		return true
	default:
		return false
	}
}

var _ streamer = &batcher{}

func sendCmd(prx *proxy, c cmd) (interface{}, error) {
	var cmdBuf bytes.Buffer
	cmdEnc := gob.NewEncoder(&cmdBuf)
	if err := cmdEnc.Encode(c); err != nil {
		return nil, err
	}
	res, err := prx.sendCmdNew(&cmdBuf)
	defer maybeCloseResponse(res)
	if err != nil {
		return nil, err
	}
	dec := gob.NewDecoder(res.Body)
	var cr cmdResult
	if err := dec.Decode(&cr); err != nil {
		return nil, err
	}
	return cr.get()
}
