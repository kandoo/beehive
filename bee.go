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

type beeStatus int

const (
	beeStatusStopped beeStatus = iota
	beeStatusJoining
	beeStatusStarted
)

type bee struct {
	sync.Mutex

	beeID     uint64
	beeColony Colony
	detached  bool
	proxy     bool
	status    beeStatus
	qee       *qee
	app       *app
	hive      *hive
	timers    []*time.Timer

	dataCh    chan msgAndHandler
	ctrlCh    chan cmdAndChannel
	handleMsg func(mh msgAndHandler)
	handleCmd func(cc cmdAndChannel)

	node       *raft.Node
	ticker     *time.Ticker
	peers      map[uint64]*proxy
	emitInRaft bool

	cells        map[CellKey]bool
	txStatus     state.TxStatus
	state        *state.Transactional
	bufferedMsgs []Msg

	local interface{}
}

func (b *bee) ID() uint64 {
	return b.beeID
}

func (b *bee) String() string {
	switch {
	case b.detached:
		return fmt.Sprintf("detached bee %v/%v/%016X", b.hive.ID(), b.app.Name(),
			b.ID())
	case b.proxy:
		return fmt.Sprintf("proxy bee %v/%v/%016X", b.hive.ID(), b.app.Name(),
			b.ID())
	default:
		return fmt.Sprintf("bee %v/%v/%016X", b.hive.ID(), b.app.Name(), b.ID())
	}
}

func (b *bee) colony() Colony {
	b.Lock()
	defer b.Unlock()

	return b.beeColony.DeepCopy()
}

func (b *bee) setColony(c Colony) {
	b.Lock()
	defer b.Unlock()

	b.beeColony = c
}
func (b *bee) setRaftNode(n *raft.Node) {
	b.Lock()
	defer b.Unlock()

	b.node = n
}

func (b *bee) raftNode() *raft.Node {
	b.Lock()
	defer b.Unlock()

	return b.node
}

func (b *bee) startNode() error {
	// TODO(soheil): restart if needed.
	c := b.colony()
	if c.IsNil() {
		return fmt.Errorf("%v is in no colony", b)
	}
	peers := make([]etcdraft.Peer, 0, 1)
	if c.Leader == b.ID() {
		peers = append(peers, raft.NodeInfo{ID: c.Leader}.Peer())
	}

	b.ticker = time.NewTicker(defaultRaftTick)
	node := raft.NewNode(b.String(), b.beeID, peers, b.sendRaft, b,
		b.statePath(), b, 1024, b.ticker.C, 10, 1)
	b.setRaftNode(node)
	// This will act like a barrier.
	if _, err := node.Process(context.TODO(), noOp{}); err != nil {
		glog.Errorf("%v cannot start raft: %v", b, err)
		return err
	}
	b.enableEmit()
	glog.V(2).Infof("%v started its raft node", b)
	return nil
}

func (b *bee) stopNode() {
	node := b.raftNode()
	if node == nil {
		return
	}
	node.Stop()
	b.disableEmit()
	b.ticker.Stop()
}

func (b *bee) enableEmit() {
	b.Lock()
	defer b.Unlock()
	b.emitInRaft = true
}

func (b *bee) disableEmit() {
	b.Lock()
	defer b.Unlock()
	b.emitInRaft = false
}

func (b *bee) ProcessStatusChange(sch interface{}) {
	switch ev := sch.(type) {
	case raft.LeaderChanged:
		glog.V(2).Infof("%v recevies leader changed event %#v", b, ev)
		if ev.New == Nil {
			// TODO(soheil): when we switch to nil during a campaign, shouldn't we
			// just change the colony?
			return
		}

		oldc := b.colony()
		if oldc.IsNil() || oldc.Leader == ev.New {
			glog.V(2).Infof("%v has no need to change %v", b, oldc)
			return
		}

		newc := oldc.DeepCopy()
		if oldc.Leader != Nil {
			newc.Leader = Nil
			newc.AddFollower(oldc.Leader)
		}
		newc.DelFollower(ev.New)
		newc.Leader = ev.New
		b.setColony(newc)

		go b.processCmd(cmdRefreshRole{})

		if ev.New == b.ID() {
			return
		}

		glog.V(2).Infof("%v is the new leader of %v", b, oldc)
		up := updateColony{
			Old: oldc,
			New: newc,
		}
		if _, err := b.hive.node.Process(context.TODO(), up); err != nil {
			glog.Errorf("%v cannot update its colony: %v", b, err)
			return
		}
		// FIXME(soheil): Add health checks here and recruite if needed.
	}
}

func (b *bee) sendRaft(msgs []raftpb.Message) {
	peerq := make(map[uint64][]raftpb.Message)
	for _, m := range msgs {
		q := peerq[m.To]
		q = append(q, m)
		peerq[m.To] = q
	}

	for to, msgs := range peerq {
		if to == b.ID() {
			glog.Fatalf("%v sends raft message to itself", b)
		}
		go b.sendRaftToPeer(to, msgs)
	}
}

func (b *bee) sendRaftToPeer(to uint64, msgs []raftpb.Message) error {
	p, err := b.raftPeer(to)
	if err != nil {
		return err
	}

	for _, m := range msgs {
		glog.V(2).Infof("%v tries to send bee raft message to %v@%v", b, m.To, p.to)
		if err = p.sendBeeRaft(b.app.Name(), m.To, m); err != nil {
			glog.Errorf("%v cannot send bee raft message to %v: %v", b, m.To, err)
			b.resetRaftPeer(to)
			return err
		}
		glog.V(2).Infof("%v successfully sent bee raft message %v to %v", b,
			m.Index, m.To)
	}

	return nil
}

func (b *bee) raftPeer(bid uint64) (*proxy, error) {
	b.Lock()
	defer b.Unlock()

	p, ok := b.peers[bid]
	if !ok {
		_, hi, err := b.hive.registry.beeAndHive(bid)
		if err != nil {
			glog.Errorf("%v cannot find the address of bee %v: %v", b, bid, err)
			return nil, err
		}
		p = newProxy(b.hive.client, hi.Addr)
		b.peers[bid] = p
	}
	return p, nil
}

func (b *bee) resetRaftPeer(bid uint64) {
	b.Lock()
	defer b.Unlock()

	delete(b.peers, bid)
}

func (b *bee) statePath() string {
	return path.Join(b.hive.config.StatePath, b.app.Name(),
		fmt.Sprintf("%016X", b.ID()))
}

func (b *bee) isLeader() bool {
	return b.beeColony.Leader == b.beeID
}

func (b *bee) addFollower(bid uint64, hid uint64) error {
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

	if err := b.raftNode().AddNode(context.TODO(), bid, ""); err != nil {
		return err
	}

	p, err := b.hive.newProxyToHive(hid)
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

func (b *bee) setState(s state.State) {
	b.state = state.NewTransactional(s)
}

func (b *bee) startDetached(h DetachedHandler) {
	if !b.detached {
		glog.Fatalf("%v is not detached", b)
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				glog.Errorf("%v recovers from an error in Start(): %v", r)
			}
		}()
		h.Start(b)
	}()
	defer h.Stop(b)

	b.start()
}

func (b *bee) start() {
	if !b.proxy && !b.colony().IsNil() && b.app.persistent() {
		if err := b.startNode(); err != nil {
			glog.Errorf("%v cannot start raft: %v", b, err)
			return
		}
	}
	b.status = beeStatusStarted
	glog.V(2).Infof("%v started", b)
	for b.status == beeStatusStarted {
		select {
		case d := <-b.dataCh:
			b.handleMsg(d)

		case c := <-b.ctrlCh:
			b.handleCmd(c)
		}
	}
}

func (b *bee) recoverFromError(mh msgAndHandler, err interface{},
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

func (b *bee) callRcv(mh msgAndHandler) {
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
	b.hive.collector.collect(b.beeID, mh.msg, b.bufferedMsgs)
}

func (b *bee) handleMsgLeader(mh msgAndHandler) {
	glog.V(2).Infof("%v handles message %v", b, mh.msg)

	if b.app.transactional() {
		b.BeginTx()
	}

	b.callRcv(mh)

	if b.app.transactional() {
		if err := b.CommitTx(); err != nil && err != state.ErrNoTx {
			glog.Errorf("%v cannot commit a transaction: %v", b, err)
		}
	}
}

func (b *bee) handleCmdLeader(cc cmdAndChannel) {
	glog.V(2).Infof("%v handles command %v", b, cc.cmd)
	var err error
	var data interface{}
	switch cmd := cc.cmd.Data.(type) {
	case cmdStop:
		b.status = beeStatusStopped
		b.stopNode()
		glog.V(2).Infof("%v stopped", b)

	case cmdStart:
		b.status = beeStatusStarted
		glog.V(2).Infof("%v started", b)

	case cmdSync:
		_, err = b.raftNode().Process(context.TODO(), noOp{})

	case cmdCampaign:
		err = b.raftNode().Campaign(context.TODO())

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
		if cmd.Colony.Leader == b.ID() {
			b.becomeLeader()
		} else {
			b.becomeFollower()
		}

	case cmdAddMappedCells:
		b.addMappedCells(cmd.Cells)

	case cmdRefreshRole:
		c := b.colony()
		if c.Leader == b.ID() {
			b.becomeLeader()
		} else {
			b.becomeFollower()
		}

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

func (b *bee) becomeLeader() {
	b.handleMsg, b.handleCmd = b.leaderHandlers()
}

func (b *bee) leaderHandlers() (func(mh msgAndHandler),
	func(cc cmdAndChannel)) {

	return b.handleMsgLeader, b.handleCmdLeader
}

func (b *bee) becomeZombie() {
	b.handleMsg, b.handleCmd = b.dropMsg, b.handleCmdLeader
}

func (b *bee) dropMsg(mh msgAndHandler) {
	glog.Errorf("%v drops %v", b, mh.msg)
}

func (b *bee) becomeFollower() {
	b.handleMsg, b.handleCmd = b.followerHandlers()
}

func (b *bee) followerHandlers() (func(mh msgAndHandler),
	func(cc cmdAndChannel)) {

	c := b.colony()
	if c.Leader == b.ID() {
		glog.Fatalf("%v is the leader")
	}

	bi, err := b.hive.registry.bee(c.Leader)
	if err != nil {
		glog.Fatalf("%v cannot find leader %v", b, c.Leader)
	}

	p, err := b.hive.newProxyToHive(bi.Hive)
	if err != nil {
		glog.Fatalf("%v cannot create proxy to %v: %v", b, bi.Hive, err)
	}

	mfn, _ := b.proxyHandlers(p, c.Leader)
	return mfn, b.handleCmdLeader
}

func (b *bee) becomeProxy(p *proxy) {
	b.proxy = true
	b.handleMsg, b.handleCmd = b.proxyHandlers(p, b.ID())
}

func (b *bee) proxyHandlers(p *proxy, to uint64) (func(mh msgAndHandler),
	func(cc cmdAndChannel)) {

	mfn := func(mh msgAndHandler) {
		glog.V(2).Infof("proxy %v sends msg %v", b, mh.msg)
		mh.msg.MsgTo = to
		if err := p.sendMsg(mh.msg); err != nil {
			glog.Errorf("cannot send message %v to %v: %v", mh.msg, b, err)
		}
	}
	cfn := func(cc cmdAndChannel) {
		switch cc.cmd.Data.(type) {
		case cmdStop, cmdStart:
			b.handleCmdLeader(cc)
		default:
			d, err := p.sendCmd(&cc.cmd)
			if cc.ch != nil {
				cc.ch <- cmdResult{Data: d, Err: err}
			}
		}
	}
	return mfn, cfn
}

func (b *bee) becomeDetached(h DetachedHandler) {
	b.detached = true
	b.handleMsg, b.handleCmd = b.detachedHandlers(h)
}

func (b *bee) detachedHandlers(h DetachedHandler) (func(mh msgAndHandler),
	func(cc cmdAndChannel)) {

	mfn := func(mh msgAndHandler) {
		h.Rcv(mh.msg, b)
	}
	return mfn, b.handleCmdLeader
}

func (b *bee) enqueMsg(mh msgAndHandler) {
	glog.V(3).Infof("%v enqueues message %v", b, mh.msg)
	for {
		select {
		case b.dataCh <- mh:
			return
		case <-time.After(60 * time.Second):
			fmt.Printf("deadline %v, %v %v, %v %v, %v %v\n", b,
				len(b.dataCh), len(b.ctrlCh),
				len(b.qee.dataCh), len(b.qee.ctrlCh),
				len(b.hive.dataCh.in()), len(b.hive.ctrlCh))
			panic("test2")
		}
	}
}

func (b *bee) enqueCmd(cc cmdAndChannel) {
	glog.V(3).Infof("%v enqueues a command %v", b, cc)
	b.ctrlCh <- cc
}

func (b *bee) processCmd(data interface{}) (interface{}, error) {
	ch := make(chan cmdResult)
	b.ctrlCh <- newCmdAndChannel(data, b.app.Name(), b.ID(), ch)
	return (<-ch).get()
}

func (b *bee) stepRaft(msg raftpb.Message) error {
	return b.raftNode().Step(context.TODO(), msg)
}

func (b *bee) mappedCells() MappedCells {
	b.Lock()
	defer b.Unlock()

	mc := make(MappedCells, 0, len(b.cells))
	for c := range b.cells {
		mc = append(mc, c)
	}
	return mc
}

func (b *bee) addMappedCells(cells MappedCells) {
	b.Lock()
	defer b.Unlock()

	if b.cells == nil {
		b.cells = make(map[CellKey]bool)
	}

	for _, c := range cells {
		glog.V(2).Infof("Adding cell %v to %v", c, b)
		b.cells[c] = true
	}
}

func (b *bee) addTimer(t *time.Timer) {
	b.Lock()
	defer b.Unlock()

	b.timers = append(b.timers, t)
}

func (b *bee) delTimer(t *time.Timer) {
	b.Lock()
	defer b.Unlock()

	for i := range b.timers {
		if b.timers[i] == t {
			b.timers = append(b.timers[:i], b.timers[i+1:]...)
			return
		}
	}
}

func (b *bee) snooze(mh msgAndHandler, d time.Duration) {
	t := time.NewTimer(d)
	b.addTimer(t)

	go func() {
		<-t.C
		b.delTimer(t)
		b.enqueMsg(mh)
	}()
}

func (b *bee) Hive() Hive {
	return b.hive
}

func (b *bee) State() State {
	return b.state
}

func (b *bee) Dict(n string) state.Dict {
	return b.State().Dict(n)
}

func (b *bee) App() string {
	return b.app.Name()
}

// Emits a message. Note that m should be your data not an instance of Msg.
func (b *bee) Emit(msgData interface{}) {
	b.bufferOrEmit(newMsgFromData(msgData, b.ID(), 0))
}

func (b *bee) doEmit(msg *msg) {
	b.hive.enqueMsg(msg)
}

func (b *bee) bufferMsg(msg *msg) {
	b.bufferedMsgs = append(b.bufferedMsgs, msg)
}

func (b *bee) bufferOrEmit(msg *msg) {
	if b.txStatus != state.TxOpen {
		b.doEmit(msg)
		return
	}

	glog.V(2).Infof("buffers msg %+v in tx", msg)
	b.bufferMsg(msg)
}

func (b *bee) SendToCellKey(msgData interface{}, to string, k CellKey) {
	// FIXME(soheil): Implement send to.
	glog.Fatal("FIXME implement bee.SendToCellKey")

	msg := newMsgFromData(msgData, b.ID(), 0)
	b.bufferOrEmit(msg)
}

func (b *bee) SendToBee(msgData interface{}, to uint64) {
	b.bufferOrEmit(newMsgFromData(msgData, b.beeID, to))
}

// Reply to msg with the provided reply.
func (b *bee) ReplyTo(msg Msg, reply interface{}) error {
	if msg.NoReply() {
		return errors.New("Cannot reply to this message.")
	}

	b.SendToBee(reply, msg.From())
	return nil
}

func (b *bee) LockCells(keys []CellKey) error {
	panic("error FIXME bee.LOCK")
}

func (b *bee) SetBeeLocal(d interface{}) {
	b.local = d
}

func (b *bee) BeeLocal() interface{} {
	return b.local
}

func (b *bee) StartDetached(h DetachedHandler) uint64 {
	d, err := b.qee.processCmd(cmdStartDetached{Handler: h})
	if err != nil {
		glog.Fatalf("Cannot start a detached bee: %v", err)
	}
	return d.(uint64)
}

func (b *bee) StartDetachedFunc(start StartFunc, stop StopFunc,
	rcv RcvFunc) uint64 {

	return b.StartDetached(&funcDetached{start, stop, rcv})
}

func (b *bee) BeginTx() error {
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

func (b *bee) emitTxMsgs() {
	if len(b.bufferedMsgs) == 0 {
		return
	}

	for _, m := range b.bufferedMsgs {
		b.doEmit(m.(*msg))
	}
}

func (b *bee) doCommitTx() error {
	defer b.resetTx()
	b.emitTxMsgs()
	if err := b.state.CommitTx(); err != nil {
		return err
	}

	return nil
}

func (b *bee) resetTx() {
	b.txStatus = state.TxNone
	b.state.Reset()
	b.bufferedMsgs = nil
}

func (b *bee) replicate() error {
	glog.V(2).Infof("%v replicates transaction", b)
	tx := b.tx()
	if len(tx.Ops) == 0 {
		return b.doCommitTx()
	}

	b.maybeRecruitFollowers()
	_, err := b.raftNode().Process(context.TODO(), commitTx(tx))
	if err == nil {
		glog.V(2).Infof("%v successfully replicates transaction", b)
	} else {
		glog.V(2).Infof("%v cannot replicate the transaction: %v", b, err)
	}
	return err
}

func (b *bee) maybeRecruitFollowers() {
	if b.detached {
		return
	}

	c := b.colony()
	if c.Leader != b.ID() {
		glog.Fatalf("%v is not master of %v and is replicating", b, c)
	}

	if n := len(c.Followers) + 1; n < b.app.replFactor {
		newf := b.doRecruitFollowers()
		if newf+n < b.app.replFactor {
			glog.Warningf("%v can replicate only on %v node(s)", b, n)
		}
	}
}

func (b *bee) doRecruitFollowers() (recruited int) {
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
		prx, err := b.hive.newProxyToHive(hives[0])
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

func (b *bee) handoff(to uint64) error {
	if !b.colony().IsFollower(to) {
		return fmt.Errorf("%v is not a follower of %v", to, b)
	}

	if _, err := b.qee.sendCmdToBee(to, cmdSync{}); err != nil {
		return err
	}
	// TODO(soheil): do we really need to stop the node here?
	b.stopNode()

	ch := make(chan error)
	go func() {
		// TODO(soheil): use context with deadline here.
		_, err := b.qee.sendCmdToBee(to, cmdCampaign{})
		ch <- err
	}()

	time.Sleep(10 * defaultRaftTick)
	if err := b.startNode(); err != nil {
		glog.Fatalf("%v cannot restart node: %v", b, err)
	}

	if b.colony().IsFollower(b.ID()) {
		glog.V(2).Infof("%v successfully handed off leadership to %v", b, to)
		b.becomeFollower()
	}
	return <-ch
}

func (b *bee) tx() Tx {
	return Tx{
		Tx:   state.Tx{Status: b.txStatus, Ops: b.state.Tx()},
		Msgs: b.bufferedMsgs,
	}
}

func (b *bee) CommitTx() error {
	if b.txStatus != state.TxOpen {
		return state.ErrNoTx
	}

	defer b.resetTx()

	// No need to update sequences.
	if !b.app.persistent() || b.detached {
		glog.V(2).Infof("%v commits in memory transaction", b)
		b.doCommitTx()
		return nil
	}

	glog.V(2).Infof("%v commits persistent transaction", b)
	return b.replicate()
}

func (b *bee) AbortTx() error {
	if b.txStatus != state.TxOpen {
		return state.ErrNoTx
	}

	glog.V(2).Infof("%v aborts tx", b)
	defer b.resetTx()
	return b.state.AbortTx()
}

func (b *bee) Snooze(d time.Duration) {
	panic(d)
}

func (b *bee) Save() ([]byte, error) {
	return b.state.Save()
}

func (b *bee) Restore(buf []byte) error {
	return b.state.Restore(buf)
}

func (b *bee) Apply(req interface{}) (interface{}, error) {
	b.Lock()
	defer b.Unlock()

	switch r := req.(type) {
	case commitTx:
		glog.V(2).Infof("%v commits %v", b, r)
		leader := b.isLeader()
		if b.txStatus == state.TxOpen {
			if !leader {
				glog.Errorf("%v is a follower and has an open transaction", b)
			}
			b.state.Reset()
		}
		if err := b.state.Apply(r.Ops); err != nil {
			return nil, err
		}

		if leader && b.emitInRaft {
			for _, m := range r.Msgs {
				msg := m.(msg)
				msg.MsgFrom = b.beeID
				glog.V(2).Infof("%v emits %#v", b, m)
				b.doEmit(&msg)
			}
		}
		return nil, nil
	case noOp:
		return nil, nil
	}
	return nil, ErrUnsupportedRequest
}

func (b *bee) ApplyConfChange(cc raftpb.ConfChange,
	n raft.NodeInfo) error {

	b.Lock()
	defer b.Unlock()

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
