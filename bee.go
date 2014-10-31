package beehive

import (
	"encoding/gob"
	"errors"
	"fmt"
	"path"
	"runtime/debug"
	"sync"
	"time"

	"github.com/kandoo/beehive/Godeps/_workspace/src/code.google.com/p/go.net/context"
	etcdraft "github.com/kandoo/beehive/Godeps/_workspace/src/github.com/coreos/etcd/raft"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/coreos/etcd/raft/raftpb"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/golang/glog"
	"github.com/kandoo/beehive/raft"
	"github.com/kandoo/beehive/state"
)

type bee interface {
	ID() uint64
	colony() Colony

	start()

	State() State
	setState(s state.State)

	enqueMsg(mh msgAndHandler)
	enqueCmd(cc cmdAndChannel)
}

type beeStatus int

const (
	beeStatusStopped beeStatus = iota
	beeStatusJoining           = iota
	beeStatusStarted           = iota
)

type localBee struct {
	m sync.Mutex

	beeID     uint64
	beeColony Colony
	detached  bool
	status    beeStatus
	qee       *qee
	app       *app
	hive      *hive
	timers    []*time.Timer

	dataCh chan msgAndHandler
	ctrlCh chan cmdAndChannel

	node   *raft.Node
	ticker *time.Ticker

	cells        map[CellKey]bool
	txStatus     state.TxStatus
	state        *state.Transactional
	bufferedMsgs []Msg

	local interface{}
}

func (b *localBee) ID() uint64 {
	return b.beeID
}

func (b *localBee) String() string {
	return fmt.Sprintf("bee %v/%v/%016X", b.hive.ID(), b.app.Name(), b.ID())
}

func (b *localBee) colony() Colony {
	b.m.Lock()
	defer b.m.Unlock()

	return b.beeColony.DeepCopy()
}

func (b *localBee) setColony(c Colony) {
	b.m.Lock()
	defer b.m.Unlock()

	b.beeColony = c
}

func (b *localBee) startNode() error {
	// TODO(soheil): restart if needed.
	c := b.colony()
	if c.IsNil() {
		return fmt.Errorf("%v is in no colony", b)
	}
	peers := make([]etcdraft.Peer, 0, 1)
	if c.Leader == b.ID() {
		peers = append(peers, raft.NodeInfo{ID: c.Leader}.Peer())
	}
	b.node = raft.NewNode(b.String(), b.beeID, peers, b.sendRaft, b,
		b.statePath(), b, 1024, b.ticker.C)
	// This will act like a barrier.
	if _, err := b.node.Process(context.TODO(), noOp{}); err != nil {
		glog.Errorf("%v cannot start raft: %v", b, err)
		return err
	}
	glog.V(2).Infof("%v started its raft node", b)
	return nil
}

func (b *localBee) ProcessStatusChange(sch interface{}) {
	switch ev := sch.(type) {
	case raft.LeaderChanged:
		glog.V(2).Infof("%v recevies leader changed event %#v", b, ev)
		if ev.New != b.ID() {
			return
		}
		oldc := b.colony()
		glog.V(2).Infof("%v is the new leader of %v", b, oldc)
		if oldc.Leader == b.ID() || oldc.IsNil() {
			glog.V(2).Infof("%v has no need to change %v", b, oldc)
			return
		}

		newc := oldc.DeepCopy()
		newc.DelFollower(b.ID())
		newc.Leader = b.ID()
		if oldc.Leader != Nil {
			newc.AddFollower(oldc.Leader)
		}
		up := updateColony{
			Old: oldc,
			New: newc,
		}
		if _, err := b.hive.node.Process(context.TODO(), up); err != nil {
			glog.Errorf("%v cannot update its colony: %v", b, err)
			return
		}
		b.setColony(newc)
		// FIXME(soheil): Add health checks here and recruite if needed.
	}
}

func (b *localBee) sendRaft(msgs []raftpb.Message) {
	var bi BeeInfo
	var hi HiveInfo
	var err error
	for _, m := range msgs {
		// TODO(soheil): Maybe launch goroutines in parallel.
		if m.To == b.ID() {
			glog.Fatalf("%v sends raft message to itself", b)
		}
		if bi, hi, err = b.hive.registry.beeAndHive(m.To); err != nil {
			glog.Errorf("cannot find bee for raft message to %v: %v", m.To, err)
			continue
		}
		if bi.ID != m.To {
			glog.Fatalf("%v receives invalid info for %v: %#v", b, m.To, bi)
		}
		glog.V(2).Infof("%v tries to send bee raft message to %v@%v", b, bi.ID,
			hi.Addr)
		if err = newProxyWithAddr(b.hive.client, hi.Addr).sendBeeRaft(bi.App, m.To,
			m); err != nil {

			glog.Errorf("%v cannot send bee raft message to %v: %v", b, m.To, err)
			continue
		}
		glog.V(2).Infof("%v successfully sent bee raft message %v to %v", b,
			m.Index, m.To)
	}
}

func (b *localBee) statePath() string {
	return path.Join(b.hive.config.StatePath, b.app.Name(),
		fmt.Sprintf("%016X", b.ID()))
}

func (b *localBee) isLeader() bool {
	return b.beeColony.Leader == b.beeID
}

func (b *localBee) addFollower(bid uint64, hid uint64) error {
	oldc := b.colony()
	if oldc.Leader != b.beeID {
		return fmt.Errorf("%v is not the leader", b)
	}
	newc := oldc.DeepCopy()
	if !newc.AddFollower(bid) {
		return ErrDuplicateBee
	}
	// TODO(soheil): It's important to have a proper order here. Or launch both in
	// parallel and cancel them on error.
	up := updateColony{
		Old: oldc,
		New: newc,
	}
	if _, err := b.hive.node.Process(context.TODO(), up); err != nil {
		glog.Errorf("%v cannot update its colony: %v", b, err)
		return err
	}

	if err := b.node.AddNode(context.TODO(), bid, ""); err != nil {
		return err
	}

	p, err := b.hive.newProxy(hid)
	if err != nil {
		return err
	}
	cmd := cmd{
		To:   bid,
		App:  b.app.Name(),
		Data: cmdJoinColony{Colony: newc},
	}
	if _, err = p.sendCmd(&cmd); err != nil {
		return err
	}

	b.setColony(newc)
	return nil
}

func (b *localBee) setState(s state.State) {
	b.state = state.NewTransactional(s)
}

func (b *localBee) start() {
	if !b.colony().IsNil() && b.app.persistent() {
		if err := b.startNode(); err != nil {
			glog.Errorf("%v cannot start raft: %v", b, err)
			return
		}
	}
	b.status = beeStatusStarted
	for b.status == beeStatusStarted {
		select {
		case d := <-b.dataCh:
			b.handleMsg(d)

		case c := <-b.ctrlCh:
			b.handleCmd(c)
		}
	}
}

func (b *localBee) recoverFromError(mh msgAndHandler, err interface{},
	stack bool) {
	b.AbortTx()

	if d, ok := err.(time.Duration); ok {
		b.snooze(mh, d)
		return
	}

	glog.Errorf("Error in %s: %v", b.app.Name(), err)
	if stack {
		glog.Errorf("%s", debug.Stack())
	}
}

func (b *localBee) callRcv(mh msgAndHandler) {
	defer func() {
		if r := recover(); r != nil {
			b.recoverFromError(mh, r, true)
		}
	}()

	if err := mh.handler.Rcv(mh.msg, b); err != nil {
		b.recoverFromError(mh, err, false)
		return
	}

	// FIXME(soheil): Provenence works when the application is transactional.
	b.hive.collector.collect(mh.msg.MsgFrom, b.beeID, mh.msg, b.bufferedMsgs)
}

func (b *localBee) handleMsg(mh msgAndHandler) {
	glog.V(2).Infof("%v handles message %v", b, mh.msg)

	if b.app.transactional() {
		b.BeginTx()
	}

	b.callRcv(mh)

	if err := b.CommitTx(); err != nil && b.app.transactional() {
		glog.Errorf("%v cannot commit a transaction : %v", b, err)
	}
}

func (b *localBee) handleCmd(cc cmdAndChannel) {
	glog.V(2).Infof("%v handles command %v", b, cc.cmd)
	var err error
	var data interface{}
	switch cmd := cc.cmd.Data.(type) {
	case cmdStop:
		b.status = beeStatusStopped
		if b.node != nil {
			b.node.Stop()
		}
		glog.V(2).Infof("%v stopped", b)

	case cmdStart:
		b.status = beeStatusStarted
		glog.V(2).Infof("%v started", b)

	case cmdSync:
		_, err = b.node.Process(context.TODO(), noOp{})

	case cmdCampaign:
		err = b.node.Campaign(context.TODO())

	case cmdHandoff:
		err = b.handoff(cmd.To)

	case cmdJoinColony:
		if !cmd.Colony.Contains(b.ID()) {
			err = fmt.Errorf("%v is not in this colony %v", b, cmd.Colony)
		}
		if !b.colony().IsNil() {
			err = fmt.Errorf("%v is already in colony %v", b, b.colony())
			break
		}
		b.setColony(cmd.Colony)
		b.startNode()

	case cmdAddFollower:
		err = b.addFollower(cmd.Bee, cmd.Hive)

	default:
		err = fmt.Errorf("Unknown bee command %#v", cmd)
		glog.Error(err.Error())
	}

	if cc.ch != nil {
		cc.ch <- cmdResult{
			Data: data,
			Err:  err,
		}
	}
}

func (b *localBee) enqueMsg(mh msgAndHandler) {
	glog.V(3).Infof("%v enqueues message %v", b, mh.msg)
	b.dataCh <- mh
}

func (b *localBee) enqueCmd(cc cmdAndChannel) {
	glog.V(3).Infof("%v enqueues a command %v", b, cc)
	b.ctrlCh <- cc
}

func (b *localBee) processCmd(data interface{}) (interface{}, error) {
	ch := make(chan cmdResult)
	b.ctrlCh <- newCmdAndChannel(data, b.app.Name(), b.ID(), ch)
	return (<-ch).get()
}

func (b *localBee) processRaft(msg raftpb.Message) error {
	return b.node.Step(context.TODO(), msg)
}

func (b *localBee) mappedCells() MappedCells {
	b.m.Lock()
	defer b.m.Unlock()

	mc := make(MappedCells, 0, len(b.cells))
	for c := range b.cells {
		mc = append(mc, c)
	}
	return mc
}

func (b *localBee) addMappedCells(cells MappedCells) {
	b.m.Lock()
	defer b.m.Unlock()

	if b.cells == nil {
		b.cells = make(map[CellKey]bool)
	}

	for _, c := range cells {
		glog.V(2).Infof("Adding cell %v to %v", c, b)
		b.cells[c] = true
	}
}

func (b *localBee) addTimer(t *time.Timer) {
	b.m.Lock()
	defer b.m.Unlock()

	b.timers = append(b.timers, t)
}

func (b *localBee) delTimer(t *time.Timer) {
	b.m.Lock()
	defer b.m.Unlock()

	for i := range b.timers {
		if b.timers[i] == t {
			b.timers = append(b.timers[:i], b.timers[i+1:]...)
			return
		}
	}
}

func (b *localBee) snooze(mh msgAndHandler, d time.Duration) {
	t := time.NewTimer(d)
	b.addTimer(t)

	go func() {
		<-t.C
		b.delTimer(t)
		b.enqueMsg(mh)
	}()
}

func (b *localBee) Hive() Hive {
	return b.hive
}

func (b *localBee) State() State {
	return b.state
}

func (b *localBee) Dict(n string) state.Dict {
	return b.State().Dict(n)
}

func (b *localBee) App() string {
	return b.app.Name()
}

// Emits a message. Note that m should be your data not an instance of Msg.
func (b *localBee) Emit(msgData interface{}) {
	b.bufferOrEmit(newMsgFromData(msgData, b.ID(), 0))
}

func (b *localBee) doEmit(msg *msg) {
	b.hive.emitMsg(msg)
}

func (b *localBee) bufferMsg(msg *msg) {
	b.bufferedMsgs = append(b.bufferedMsgs, msg)
}

func (b *localBee) bufferOrEmit(msg *msg) {
	if b.txStatus != state.TxOpen {
		b.doEmit(msg)
		return
	}

	glog.V(2).Infof("Buffers msg %+v in tx", msg)
	b.bufferMsg(msg)
}

func (b *localBee) SendToCellKey(msgData interface{}, to string, k CellKey) {
	// FIXME(soheil): Implement send to.
	glog.Fatal("FIXME implement bee.SendToCellKey")

	msg := newMsgFromData(msgData, b.ID(), 0)
	b.bufferOrEmit(msg)
}

func (b *localBee) SendToBee(msgData interface{}, to uint64) {
	b.bufferOrEmit(newMsgFromData(msgData, b.beeID, to))
}

// Reply to msg with the provided reply.
func (b *localBee) ReplyTo(msg Msg, reply interface{}) error {
	if msg.NoReply() {
		return errors.New("Cannot reply to this message.")
	}

	b.SendToBee(reply, msg.From())
	return nil
}

func (b *localBee) Lock(keys []CellKey) error {
	panic("error FIXME bee.LOCK")
}

func (b *localBee) SetBeeLocal(d interface{}) {
	b.local = d
}

func (b *localBee) BeeLocal() interface{} {
	return b.local
}

func (b *localBee) StartDetached(h DetachedHandler) uint64 {
	d, err := b.qee.processCmd(cmdStartDetached{Handler: h})
	if err != nil {
		glog.Fatalf("Cannot start a detached bee: %v", err)
	}
	return d.(uint64)
}

func (b *localBee) StartDetachedFunc(start StartFunc, stop StopFunc,
	rcv RcvFunc) uint64 {

	return b.StartDetached(&funcDetached{start, stop, rcv})
}

func (b *localBee) BeginTx() error {
	if b.txStatus == state.TxOpen {
		return state.ErrOpenTx
	}

	if err := b.state.BeginTx(); err != nil {
		glog.Errorf("Cannot begin a transaction for %v: %v", b, err)
		return err
	}

	b.txStatus = state.TxOpen
	glog.V(2).Infof("%v begins a new transaction", b)
	return nil
}

func (b *localBee) emitTxMsgs() {
	if len(b.bufferedMsgs) == 0 {
		return
	}

	for _, m := range b.bufferedMsgs {
		b.doEmit(m.(*msg))
	}
}

func (b *localBee) doCommitTx() error {
	defer b.resetTx()
	b.emitTxMsgs()
	if err := b.state.CommitTx(); err != nil {
		return err
	}

	return nil
}

func (b *localBee) resetTx() {
	b.txStatus = state.TxNone
	b.state.Reset()
	b.bufferedMsgs = nil
}

func (b *localBee) replicate() error {
	glog.V(2).Infof("%v replicates transaction", b)
	tx := b.tx()
	if len(tx.Ops) == 0 {
		return b.doCommitTx()
	}

	if n := len(b.colony().Followers) + 1; n < b.app.replFactor {
		newf := b.recruiteFollowers()
		if newf+n < b.app.replFactor {
			glog.Warningf("%v can replicate only on %v node(s)", b, n)
		}
	}

	_, err := b.node.Process(context.TODO(), commitTx(tx))
	if err == nil {
		glog.V(2).Infof("%v successfully replicates transaction", b)
	} else {
		glog.V(2).Infof("%v cannot replicate the transaction: %v", b, err)
	}
	return err
}

func (b *localBee) recruiteFollowers() (recruited int) {
	c := b.colony()
	r := b.app.replFactor - len(c.Followers)
	if r == 1 {
		return 0
	}

	blacklist := []uint64{b.hive.ID()}
	for _, f := range b.colony().Followers {
		fb, err := b.hive.registry.bee(f)
		if err != nil {
			glog.Fatalf("%v cannot find the hive of follower %v", b, fb)
		}
		blacklist = append(blacklist, fb.Hive)
	}
	for r != 1 {
		// TODO(soheil): We can try to select multiple hives at once.
		hives := b.hive.replStrategy.selectHives(blacklist, 1)
		if len(hives) == 0 {
			glog.Warningf("can only find %v hives to create followers for %v",
				len(b.colony().Followers), b)
			break
		}

		blacklist = append(blacklist, hives[0])
		prx, err := b.hive.newProxy(hives[0])
		if err != nil {
			continue
		}
		glog.V(2).Infof("trying to create a new follower for %v on hive %v", b,
			hives[0])
		res, err := prx.sendCmd(&cmd{
			App:  b.app.Name(),
			Data: cmdCreateBee{},
		})
		if err != nil {
			glog.Errorf("%v cannot create a new bee on %v: %v", b, hives[0], err)
			continue
		}
		fid := res.(uint64)
		if err = b.addFollower(fid, hives[0]); err != nil {
			glog.Errorf("%v cannot add %v as a follower: %v", b, fid, err)
			continue
		}
		recruited++
		r--
	}

	glog.V(2).Infof("%v recruited %d followers", b, recruited)
	return recruited
}

func (b *localBee) handoff(to uint64) error {
	if !b.colony().IsFollower(to) {
		return fmt.Errorf("%v is not a follower of %v", to, b)
	}

	if _, err := b.qee.sendCmdToBee(to, cmdSync{}); err != nil {
		return err
	}
	b.node.Stop()

	ch := make(chan error)
	go func() {
		_, err := b.qee.sendCmdToBee(to, cmdCampaign{})
		ch <- err
	}()

	time.Sleep(10 * defaultRaftTick)
	if err := b.startNode(); err != nil {
		glog.Fatalf("%v cannot restart node: %v", b, err)
	}
	return <-ch
}

func (b *localBee) tx() Tx {
	return Tx{
		Tx:   state.Tx{Status: b.txStatus, Ops: b.state.Tx()},
		Msgs: b.bufferedMsgs,
	}
}

func (b *localBee) CommitTx() error {
	if b.txStatus != state.TxOpen {
		return state.ErrNoTx
	}

	glog.V(2).Infof("%v commits transaction", b)

	defer b.resetTx()

	// No need to update sequences.
	if !b.app.persistent() || b.detached {
		b.doCommitTx()
		return nil
	}

	return b.replicate()
}

func (b *localBee) AbortTx() error {
	if b.txStatus != state.TxOpen {
		return state.ErrNoTx
	}

	glog.V(2).Infof("%v aborts tx", b)
	defer b.resetTx()
	return b.state.AbortTx()
}

func (b *localBee) Snooze(d time.Duration) {
	panic(d)
}

func (b *localBee) Save() ([]byte, error) {
	return b.state.Save()
}

func (b *localBee) Restore(buf []byte) error {
	return b.state.Restore(buf)
}

func (b *localBee) Apply(req interface{}) (interface{}, error) {
	b.m.Lock()
	defer b.m.Unlock()

	switch r := req.(type) {
	case commitTx:
		glog.V(2).Infof("%v commits %+v", b, r)
		leader := b.isLeader()
		if b.txStatus == state.TxOpen {
			if !leader {
				glog.Errorf("Follower %v has an open transaction", b)
			}
			b.state.Reset()
		}
		if err := b.state.Apply(r.Ops); err != nil {
			return nil, err
		}

		if leader {
			for _, m := range r.Msgs {
				msg := m.(msg)
				msg.MsgFrom = b.beeID
				b.doEmit(&msg)
			}
		}
		return nil, nil
	case noOp:
		return nil, nil
	}
	return nil, ErrUnsupportedRequest
}

func (b *localBee) ApplyConfChange(cc raftpb.ConfChange,
	n raft.NodeInfo) error {

	b.m.Lock()
	defer b.m.Unlock()

	col := b.beeColony
	switch cc.Type {
	case raftpb.ConfChangeAddNode:
		if col.Contains(cc.NodeID) {
			return ErrDuplicateBee
		}
		col.AddFollower(cc.NodeID)
	case raftpb.ConfChangeRemoveNode:
		if !col.Contains(cc.NodeID) {
			return ErrNoSuchBee
		}
		if cc.NodeID == b.beeID {
			// TODO(soheil): Should we stop the bee here?
			glog.Fatalf("bee is alive but removed from raft")
		}
		if col.Leader == cc.NodeID {
			// TODO(soheil): Should we launch a goroutine to campaign here?
			col.Leader = 0
		} else {
			col.DelFollower(cc.NodeID)
		}
	}
	b.beeColony = col
	return nil
}

// bee raft commands
type commitTx Tx

func init() {
	gob.Register(commitTx{})
}
