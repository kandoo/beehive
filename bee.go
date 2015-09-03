package beehive

import (
	"encoding/gob"
	"errors"
	"fmt"
	"path"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	etcdraft "github.com/kandoo/beehive/Godeps/_workspace/src/github.com/coreos/etcd/raft"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/coreos/etcd/raft/raftpb"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/golang/glog"
	"github.com/kandoo/beehive/Godeps/_workspace/src/golang.org/x/net/context"

	"github.com/kandoo/beehive/bucket"
	"github.com/kandoo/beehive/raft"
	"github.com/kandoo/beehive/state"
)

type beeStatus int

const (
	beeStatusStopped beeStatus = iota
	beeStatusJoining
	beeStatusStarted
)

var (
	ErrOldTx       = errors.New("transaction has an old term")
	ErrIsNotMaster = errors.New("bee is not master")
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
	cells     map[CellKey]bool

	dataCh    *msgChannel
	outCh     chan []*msg
	ctrlCh    chan cmdAndChannel
	handleMsg func(mhs []msgAndHandler)
	handleCmd func(cc cmdAndChannel)
	batchSize uint
	prxClient clientBackoff

	inBucket  *bucket.Bucket
	outBucket *bucket.Bucket

	emitInRaft bool
	raftTerm   uint64
	txTerm     uint64

	stateL1  *state.Transactional
	stateL2  *state.Transactional
	msgBufL1 []*msg
	msgBufL2 []*msg

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
	c := b.beeColony.DeepCopy()
	b.Unlock()
	return c
}

func (b *bee) setColony(c Colony) {
	b.Lock()
	b.beeColony = c
	b.Unlock()
}

func (b *bee) isColonyNil() bool {
	b.Lock()
	n := b.beeColony.IsNil()
	b.Unlock()
	return n
}

func (b *bee) isFollower(bid uint64) bool {
	b.Lock()
	f := b.beeColony.IsFollower(bid)
	b.Unlock()
	return f
}

func (b *bee) term() uint64 {
	return atomic.LoadUint64(&b.raftTerm)
}

func (b *bee) setTerm(term uint64) {
	// TODO(soheil): Maybe check whether there the term is valid.
	atomic.StoreUint64(&b.raftTerm, term)
}

func (b *bee) peer(gid, bid uint64) etcdraft.Peer {
	bi, err := b.hive.registry.bee(bid)
	if err != nil {
		glog.Fatalf("%v cannot find peer bee %v: %v", b, bid, err)
	}
	// TODO(soheil): maybe include address.
	return raft.GroupNode{Node: bi.Hive, Group: gid, Data: bid}.Peer()
}

func (b *bee) peers() (ps []etcdraft.Peer) {
	c := b.colony()
	if c.IsNil() {
		return []etcdraft.Peer{}
	}

	if c.Leader == b.ID() {
		ps = append(ps, b.peer(c.ID, c.Leader))
	}
	return
}

func (b *bee) createGroup() error {
	c := b.colony()
	if c.IsNil() || c.ID == Nil {
		return fmt.Errorf("%v is in no colony", b)
	}
	cfg := raft.GroupConfig{
		ID:             c.ID,
		Name:           b.String(),
		StateMachine:   b,
		Peers:          b.peers(),
		DataDir:        b.statePath(),
		SnapCount:      1024,
		FsyncTick:      b.hive.config.RaftFsyncTick,
		ElectionTicks:  b.hive.config.RaftElectTicks,
		HeartbeatTicks: b.hive.config.RaftHBTicks,
		MaxInFlights:   b.hive.config.RaftInFlights,
		MaxMsgSize:     b.hive.config.RaftMaxMsgSize,
	}
	if err := b.hive.node.CreateGroup(context.TODO(), cfg); err != nil {
		return err
	}

	if err := b.raftBarrier(); err != nil {
		return err
	}

	b.enableEmit()
	glog.V(2).Infof("%v started its raft node", b)
	return nil
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
		oldi, err := b.hive.bee(oldc.Leader)
		if err != nil {
			glog.Fatalf("%v cannot find leader: %v", b, err)
		}
		if oldi.Hive == ev.New {
			glog.V(2).Infof("%v has no need to change %v", b, oldc)
			return
		}

		newc := oldc.DeepCopy()
		if oldc.Leader != Nil {
			newc.Leader = Nil
			newc.AddFollower(oldc.Leader)
		}
		newi := b.fellowBeeOnHive(ev.New)
		newc.DelFollower(newi.ID)
		newc.Leader = newi.ID
		b.setColony(newc)

		go b.processCmd(cmdRefreshRole{})

		if ev.New != b.hive.ID() {
			return
		}

		b.setTerm(ev.Term)

		go func() {
			// FIXME(): add raft term to make sure it's versioned.
			glog.V(2).Infof("%v is the new leader of %v", b, oldc)
			up := updateColony{
				Term: ev.Term,
				Old:  oldc,
				New:  newc,
			}

			// TODO(soheil): should we have a max retry?
			_, err := b.hive.node.ProposeRetry(hiveGroup, up,
				b.hive.config.RaftElectTimeout(), -1)
			if err != nil {
				glog.Errorf("%v cannot update its colony: %v", b, err)
			}
		}()
		// TODO(soheil): add health checks here and recruit if needed.
	}
}

func (b *bee) fellowBeeOnHive(hive uint64) (fellow BeeInfo) {
	c := b.colony()
	i, err := b.hive.bee(c.Leader)
	if err != nil {
		glog.Fatalf("%v cannot find leader %v", b, c.Leader)
	}
	if i.Hive == hive {
		return i
	}
	for _, f := range c.Followers {
		i, err = b.hive.bee(f)
		if err != nil {
			glog.Fatalf("%v cannot find bee %v", b, f)
		}
		if i.Hive == hive {
			return i
		}
	}
	glog.Fatalf("%v cannot find fellow on hive %v", b, hive)
	return
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

	t := 10 * b.hive.config.RaftElectTimeout()
	upctx, upcnl := context.WithTimeout(context.Background(), t)
	defer upcnl()

	gid := oldc.ID
	// TODO(soheil): It's important to have a proper order here. Or launch both in
	// parallel and cancel them on error.
	up := updateColony{
		Term: b.term(),
		Old:  oldc,
		New:  newc,
	}
	if _, err := b.hive.proposeAmongHives(upctx, up); err != nil {
		glog.Errorf("%v cannot update its colony: %v", b, err)
		return err
	}

	cfgctx, cfgcnl := context.WithTimeout(context.Background(), t)
	defer cfgcnl()
	if err := b.hive.node.AddNodeToGroup(cfgctx, hid, gid, bid); err != nil {
		return err
	}

	cmd := cmd{
		Hive: hid,
		App:  b.app.Name(),
		Bee:  bid,
		Data: cmdJoinColony{Colony: newc},
	}
	if _, err := b.hive.client.sendCmd(cmd); err != nil {
		return err
	}

	b.setColony(newc)
	return nil
}

func (b *bee) setState(s state.State) {
	b.stateL1 = state.NewTransactional(s)
}

func (b *bee) startDetached(h DetachedHandler) {
	if !b.detached {
		glog.Fatalf("%v is not detached", b)
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				glog.Errorf("%v recovers from an error in Start(): %v", b, r)
			}
		}()
		h.Start(b)
	}()
	defer h.Stop(b)

	b.start()
}

func (b *bee) start() {
	if !b.proxy && !b.isColonyNil() && b.app.persistent() {
		if err := b.createGroup(); err != nil {
			glog.Errorf("%v cannot start raft: %v", b, err)
			return
		}
	}

	b.status = beeStatusStarted
	glog.V(2).Infof("%v started", b)

	dataCh := b.dataCh.out()
	batch := make([]msgAndHandler, 0, b.batchSize)

	outCh := b.outCh
	var outM []*msg
	if b.outBucket.Unlimited() {
		outM = nil
	}

	b.inBucket.Reset()
	b.outBucket.Reset()
	var inT <-chan time.Time
	var outT <-chan time.Time

	for b.status == beeStatusStarted {
		select {
		case mh := <-dataCh:
			batch = append(batch, mh)
		loop:
			for uint(len(batch)) < b.batchSize {
				select {
				case mh = <-dataCh:
					batch = append(batch, mh)
				default:
					break loop
				}
			}

			t := uint64(len(batch))
			if !b.inBucket.Get(t) {
				dataCh = nil
				inT = time.After(b.inBucket.When(t))
				break
			}

			b.handleMsg(batch)
			batch = clearBatch(batch)

		case <-inT:
			if !b.inBucket.Get(uint64(len(batch))) {
				glog.Fatalf("cannot get tokens after the wait")
			}
			b.handleMsg(batch)
			batch = clearBatch(batch)
			dataCh = b.dataCh.out()
			inT = nil

		case outM = <-outCh:
			l := uint64(len(outM))
			if b.outBucket.Get(l) {
				b.doEmit(outM)
				break
			}
			outT = time.After(b.outBucket.When(uint64(len(outM))))
			outCh = nil

		case <-outT:
			if !b.outBucket.Get(uint64(len(outM))) {
				glog.Fatalf("cannot get tokens after wait")
			}
			b.doEmit(outM)
			outCh = b.outCh
			outM = nil
			outT = nil

		case c := <-b.ctrlCh:
			b.handleCmd(c)
		}
	}
}

func clearBatch(batch []msgAndHandler) []msgAndHandler {
	for i := range batch {
		batch[i].msg = nil
	}
	return batch[0:0]
}

func (b *bee) recoverFromError(mh msgAndHandler, err interface{},
	stack bool) {
	b.AbortTx()

	if d, ok := err.(time.Duration); ok {
		b.snooze(mh, d)
		return
	}

	glog.Errorf("error in %s for %s: %v", b.app.Name(), mh.msg.Type(), err)
	if stack {
		glog.Errorf("%s", debug.Stack())
	}
}

var (
	errRcv = errors.New("error in rcv")
)

func (b *bee) callRcv(mh msgAndHandler) (err error) {
	defer func() {
		if r := recover(); r != nil {
			b.recoverFromError(mh, r, true)
		}
		err = errRcv
	}()

	if err := mh.handler.Rcv(mh.msg, b); err != nil {
		b.recoverFromError(mh, err, false)
		return errRcv
	}

	// FIXME(soheil): Provenence works only when the application is transactional.
	var msgs []*msg
	if b.stateL2 != nil {
		msgs = b.msgBufL2
	} else {
		msgs = b.msgBufL1
	}
	b.hive.collector.collect(b.beeID, mh.msg, msgs)
	return nil
}

func (b *bee) handleMsgLeader(mhs []msgAndHandler) {

	usetx := b.app.transactional()
	if usetx && len(mhs) > 1 {
		b.stateL2 = state.NewTransactional(b.stateL1)
		b.stateL1.BeginTx()
	}

	for i := range mhs {
		if usetx {
			b.BeginTx()
		}

		mh := mhs[i]
		if glog.V(2) {
			glog.Infof("%v handles message %v", b, mh.msg)
		}
		b.callRcv(mh)

		if usetx {
			var err error
			if b.stateL2 == nil {
				err = b.CommitTx()
			} else if len(b.msgBufL1) == 0 && b.stateL2.HasEmptyTx() {
				// If there is no pending L1 message and there is no state change,
				// emit the buffered messages in L2 as a shortcut.
				b.throttle(b.msgBufL2)
				b.resetTx(b.stateL2, &b.msgBufL2)
			} else {
				err = b.commitTxL2()
			}

			if err != nil && err != state.ErrNoTx {
				glog.Errorf("%v cannot commit a transaction: %v", b, err)
			}
		}
	}

	if !usetx || b.stateL2 == nil {
		return
	}

	b.stateL2 = nil
	if err := b.CommitTx(); err != nil && err != state.ErrNoTx {
		glog.Errorf("%v cannot commit a transaction: %v", b, err)
	}
}

func (b *bee) group() uint64 {
	b.Lock()
	g := b.beeColony.ID
	b.Unlock()
	return g
}

func (b *bee) handleCmdLocal(cc cmdAndChannel) {
	glog.V(2).Infof("%v handles command %v", b, cc.cmd)
	var err error
	var data interface{}
	switch cmd := cc.cmd.Data.(type) {
	case cmdStop:
		b.status = beeStatusStopped
		b.disableEmit()
		glog.V(2).Infof("%v stopped", b)

	case cmdStart:
		b.status = beeStatusStarted
		glog.V(2).Infof("%v started", b)

	case cmdSync:
		err = b.raftBarrier()

	case cmdRestoreState:
		err = b.stateL1.Restore(cmd.State)

	case cmdCampaign:
		ctx, cnl := context.WithTimeout(context.Background(),
			b.hive.config.RaftElectTimeout())
		err = b.hive.node.Campaign(ctx, b.group())
		cnl()

	case cmdHandoff:
		err = b.handoff(cmd.To)

	case cmdJoinColony:
		if !cmd.Colony.Contains(b.ID()) {
			err = fmt.Errorf("%v is not in this colony %v", b, cmd.Colony)
			break
		}
		if !b.isColonyNil() {
			err = fmt.Errorf("%v is already in colony %v", b, b.colony())
			break
		}
		b.setColony(cmd.Colony)
		if b.app.persistent() {
			if err = b.createGroup(); err != nil {
				break
			}
		}
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
		err = fmt.Errorf("unknown bee command %#v", cmd)
	}

	if err != nil {
		glog.Errorf("%v cannot handle %v: %v", b, cc.cmd, err)
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

func (b *bee) leaderHandlers() (func(mhs []msgAndHandler),
	func(cc cmdAndChannel)) {

	return b.handleMsgLeader, b.handleCmdLocal
}

func (b *bee) becomeZombie() {
	b.handleMsg, b.handleCmd = b.dropMsg, b.handleCmdLocal
}

func (b *bee) dropMsg(mhs []msgAndHandler) {
	glog.Errorf("%v drops %v", b, mhs)
}

func (b *bee) becomeFollower() {
	b.handleMsg, b.handleCmd = b.followerHandlers()
}

func (b *bee) followerHandlers() (func(mhs []msgAndHandler),
	func(cc cmdAndChannel)) {

	c := b.colony()
	if c.Leader == b.ID() {
		glog.Fatalf("%v is the leader", b)
	}

	_, err := b.hive.registry.bee(c.Leader)
	if err != nil {
		glog.Fatalf("%v cannot find leader %v", b, c.Leader)
	}

	mfn, _ := b.proxyHandlers(c.Leader)
	return mfn, b.handleCmdLocal
}

func (b *bee) becomeProxy() {
	b.proxy = true
	b.handleMsg, b.handleCmd = b.proxyHandlers(b.ID())
}

func (b *bee) proxyHandlers(to uint64) (func(mhs []msgAndHandler),
	func(cc cmdAndChannel)) {

	bi, err := b.hive.bee(to)
	if err != nil {
		glog.Fatalf("cannot find bee %v: %v", to, err)
	}

	mfn := func(mhs []msgAndHandler) {
		if !b.prxClient.backoff.Equal(time.Time{}) &&
			time.Now().Before(b.prxClient.backoff) {

			glog.Errorf("%v cannot send message: backing off", b)
			return
		}

		if b.prxClient.client == nil {
			c, err := b.hive.client.beeClient(to)
			if err != nil {
				if berr, ok := err.(*rpcBackoffError); ok {
					b.prxClient = clientBackoff{backoff: berr.Until}
				}
				glog.Errorf("%v cannot send message: %v", b, err)
				return
			}
			b.prxClient = clientBackoff{client: c}
		}

		msgs := make([]msg, 0, len(mhs))
		for i := range mhs {
			msg := *(mhs[i].msg)
			msg.MsgTo = to
			msgs = append(msgs, msg)
		}

		for {
			if err := b.prxClient.client.sendMsg(msgs); err == nil {
				return
			}

			// Maybe a second try, if the previous connection is closed.
			if b.prxClient.client, err = b.hive.client.resetBeeClient(to,
				b.prxClient.client); err != nil {

				glog.Errorf("%v cannot send message: %v", b, err)
				return
			}
		}
	}

	cfn := func(cc cmdAndChannel) {
		switch cc.cmd.Data.(type) {
		case cmdStop, cmdStart:
			b.handleCmdLocal(cc)
		default:
			cc.cmd.Hive = bi.Hive
			cc.cmd.App = bi.App
			cc.cmd.Bee = to
			// TODO(soheil): maybe use prxClient here as well.
			res, err := b.hive.client.sendCmd(cc.cmd)
			if cc.ch != nil {
				cc.ch <- cmdResult{Data: res, Err: err}
			}
		}
	}
	return mfn, cfn
}

func (b *bee) becomeDetached(h DetachedHandler) {
	b.detached = true
	b.handleMsg, b.handleCmd = b.detachedHandlers(h)
}

func (b *bee) detachedHandlers(h DetachedHandler) (func(mhs []msgAndHandler),
	func(cc cmdAndChannel)) {

	mfn := func(mhs []msgAndHandler) {
		for i := range mhs {
			h.Rcv(mhs[i].msg, b)
		}
	}
	return mfn, b.handleCmdLocal
}

func (b *bee) enqueMsg(mh msgAndHandler) {
	glog.V(3).Infof("%v enqueues message %v", b, mh.msg)
	b.dataCh.in() <- mh
}

func (b *bee) enqueCmd(cc cmdAndChannel) {
	glog.V(3).Infof("%v enqueues a command %v", b, cc)
	b.ctrlCh <- cc
}

func (b *bee) processCmd(data interface{}) (interface{}, error) {
	ch := make(chan cmdResult)
	b.ctrlCh <- newCmdAndChannel(data, b.hive.ID(), b.app.Name(), b.ID(), ch)
	return (<-ch).get()
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

func (b *bee) Dict(n string) state.Dict {
	dicts, _ := b.currentState()
	return dicts.Dict(n)
}

func (b *bee) App() string {
	return b.app.Name()
}

// Emits a message. Note that m should be your data not an instance of Msg.
func (b *bee) Emit(msgData interface{}) {
	b.bufferOrEmit(newMsgFromData(msgData, b.ID(), 0))
}

func (b *bee) doEmit(msgs []*msg) {
	for i := range msgs {
		b.hive.enqueMsg(msgs[i])
	}
}

func (b *bee) throttle(msgs []*msg) {
	if b.outBucket.Unlimited() {
		b.doEmit(msgs)
		return
	}

	max := b.outBucket.Max()
	len := uint64(len(msgs))
	for {
		if len == 0 {
			return
		}
		if len <= max {
			b.outCh <- msgs
			return
		}

		b.outCh <- msgs[:max]
		msgs = msgs[max:]
		len -= max
	}
}

func (b *bee) bufferOrEmit(m *msg) {
	dicts, msgs := b.currentState()
	if dicts.TxStatus() != state.TxOpen {
		b.throttle([]*msg{m})
		return
	}

	glog.V(2).Infof("buffers msg %+v in tx", m)
	*msgs = append(*msgs, m)
}

func (b *bee) SendToCell(msgData interface{}, app string, cell CellKey) {
	bi, _, err := b.hive.registry.beeForCells(app, MappedCells{cell})
	if err != nil {
		glog.Fatalf("cannot find any bee in app %v for cell %v", app, cell)
	}
	msg := newMsgFromData(msgData, bi.ID, 0)
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

func (b *bee) DeferReply(msg Msg) Repliable {
	return Repliable{From: msg.From()}
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

func (b *bee) Sync(ctx context.Context, req interface{}) (res interface{},
	err error) {

	return b.hive.Sync(ctx, req)
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
	dicts, _ := b.currentState()
	if dicts.TxStatus() == state.TxOpen {
		return state.ErrOpenTx
	}

	if err := dicts.BeginTx(); err != nil {
		glog.Errorf("Cannot begin a transaction for %v: %v", b, err)
		return err
	}

	glog.V(2).Infof("%v begins a new transaction", b)
	return nil
}

func (b *bee) commitTxBothLayers() (err error) {
	hasL2 := b.stateL2 != nil
	if hasL2 {
		if err = b.stateL2.CommitTx(); err != nil {
			goto reset
		}
	}

	if err = b.stateL1.CommitTx(); err != nil {
		goto reset
	}

	b.throttle(b.msgBufL1)
	if hasL2 {
		b.throttle(b.msgBufL2)
	}

reset:
	if hasL2 {
		b.resetTx(b.stateL2, &b.msgBufL2)
	}
	b.resetTx(b.stateL1, &b.msgBufL1)
	return
}

func (b *bee) commitTxL1() (err error) {
	if b.stateL2 != nil {
		glog.Errorf("%v has open L2 transaction while committing L1", b)
		b.commitTxL2()
	}

	if err = b.stateL1.CommitTx(); err == nil {
		b.throttle(b.msgBufL1)
	}
	b.resetTx(b.stateL1, &b.msgBufL1)
	return
}

func (b *bee) commitTxL2() (err error) {
	if b.stateL2 == nil {
		return state.ErrNoTx
	}
	if err = b.stateL2.CommitTx(); err == nil {
		b.msgBufL1 = append(b.msgBufL1, b.msgBufL2...)
	}
	b.resetTx(b.stateL2, &b.msgBufL2)
	return
}

func (b *bee) resetTx(dicts *state.Transactional, msgs *[]*msg) {
	dicts.Reset()
	for i := range *msgs {
		(*msgs)[i] = nil
	}
	*msgs = (*msgs)[:0]
}

func (b *bee) replicate() error {
	glog.V(2).Infof("%v replicates transaction", b)
	b.Lock()

	if b.stateL2 != nil {
		err := b.commitTxL2()
		b.stateL2 = nil
		if err != nil && err != state.ErrNoTx {
			b.Unlock()
			return err
		}
	}

	if b.stateL1.TxStatus() != state.TxOpen {
		b.Unlock()
		return state.ErrNoTx
	}

	stx := b.stateL1.Tx()
	if len(stx.Ops) == 0 {
		err := b.commitTxL1()
		b.Unlock()
		return err
	}

	b.Unlock()

	if err := b.maybeRecruitFollowers(); err != nil {
		return err
	}

	msgs := make([]*msg, len(b.msgBufL1))
	copy(msgs, b.msgBufL1)
	tx := tx{
		Tx:   stx,
		Msgs: msgs,
	}
	ctx, cnl := context.WithTimeout(context.Background(),
		10*b.hive.config.RaftElectTimeout())
	defer cnl()
	commit := commitTx{
		Tx:   tx,
		Term: b.term(),
	}
	if _, err := b.hive.node.Propose(ctx, b.group(), commit); err != nil {
		glog.Errorf("%v cannot replicate the transaction: %v", b, err)
		return err
	}
	glog.V(2).Infof("%v successfully replicates transaction", b)
	return nil
}

func (b *bee) maybeRecruitFollowers() error {
	if b.detached {
		return nil
	}

	c := b.colony()
	if c.Leader != b.ID() {
		return ErrIsNotMaster
	}

	if n := len(c.Followers) + 1; n < b.app.replFactor {
		newf := b.doRecruitFollowers()
		if newf+n < b.app.replFactor {
			glog.Warningf("%v can replicate only on %v node(s)", b, n)
		}
	}

	return nil
}

func (b *bee) doRecruitFollowers() (recruited int) {
	c := b.colony()
	r := b.app.replFactor - len(c.Followers)
	if r == 1 {
		return 0
	}

	blacklist := []uint64{b.hive.ID()}
	for _, f := range c.Followers {
		fb, err := b.hive.registry.bee(f)
		if err != nil {
			glog.Fatalf("%v cannot find the hive of follower %v: %v", b, f, err)
		}
		blacklist = append(blacklist, fb.Hive)
	}

	for r != 1 {
		hives := b.hive.replStrategy.selectHives(blacklist, r-1)
		if len(hives) == 0 {
			glog.Warningf("can only find %v hives to create followers for %v",
				len(c.Followers), b)
			break
		}

		fch := make(chan BeeInfo)

		tries := r - 1
		if len(hives) < tries {
			tries = len(hives)
		}

		for i := 0; i < tries; i++ {
			blacklist = append(blacklist, hives[i])
			go func(i int) {
				glog.V(2).Infof("trying to create a new follower for %v on hive %v", b,
					hives[0])
				cmd := cmd{
					Hive: hives[i],
					App:  b.app.Name(),
					Data: cmdCreateBee{},
				}
				res, err := b.hive.client.sendCmd(cmd)
				if err != nil {
					glog.Errorf("%v cannot create a new bee on %v: %v", b, hives[0], err)
					fch <- BeeInfo{}
					return
				}
				fch <- BeeInfo{
					ID:   res.(uint64),
					Hive: hives[i],
				}
			}(i)
		}

		for i := 0; i < tries; i++ {
			finf := <-fch
			if finf.ID == 0 {
				continue
			}

			if err := b.addFollower(finf.ID, finf.Hive); err != nil {
				glog.Errorf("%v cannot add %v as a follower: %v", b, finf.ID, err)
				continue
			}
			recruited++
			r--
		}
	}

	glog.V(2).Infof("%v recruited %d followers", b, recruited)
	return recruited
}

func (b *bee) handoffNonPersistent(to uint64) error {
	s, err := b.stateL1.Save()
	if err != nil {
		return err
	}

	if _, err = b.qee.sendCmdToBee(to, cmdRestoreState{State: s}); err != nil {
		return err
	}

	oldc := b.colony()
	newc := oldc.DeepCopy()
	newc.Leader = to
	up := updateColony{
		Term: b.term(),
		Old:  oldc,
		New:  newc,
	}
	ctx, cnl := context.WithTimeout(context.Background(),
		10*b.hive.config.RaftElectTimeout())
	defer cnl()
	if _, err := b.hive.proposeAmongHives(ctx, up); err != nil {
		return err
	}

	b.becomeProxy()
	return nil
}

func (b *bee) handoff(to uint64) error {
	if !b.app.persistent() {
		return b.handoffNonPersistent(to)
	}

	c := b.colony()
	if !c.IsFollower(to) {
		return fmt.Errorf("%v is not a follower of %v", to, b)
	}

	if _, err := b.qee.sendCmdToBee(to, cmdSync{}); err != nil {
		return err
	}

	ch := make(chan error)
	go func() {
		// TODO(soheil): use context with deadline here.
		_, err := b.qee.sendCmdToBee(to, cmdCampaign{})
		ch <- err
	}()

	t := b.hive.config.RaftElectTimeout()
	time.Sleep(t)
	if _, err := b.hive.node.ProposeRetry(c.ID, noOp{}, t, 10); err != nil {
		glog.Errorf("%v cannot sync raft: %v", b, err)
	}

	if b.isFollower(b.ID()) {
		glog.V(2).Infof("%v successfully handed off leadership to %v", b, to)
		b.becomeFollower()
	}
	return <-ch
}

func (b *bee) raftBarrier() error {
	_, err := b.hive.node.ProposeRetry(b.group(), noOp{},
		10*b.hive.config.RaftElectTimeout(), -1)
	return err
}

func (b *bee) currentState() (dicts *state.Transactional, msgs *[]*msg) {
	if b.stateL2 != nil {
		dicts = b.stateL2
		msgs = &b.msgBufL2
	} else {
		dicts = b.stateL1
		msgs = &b.msgBufL1
	}
	return
}

func (b *bee) CommitTx() error {
	// No need to replicate and/or persist the transaction.
	if !b.app.persistent() || b.detached {
		glog.V(2).Infof("%v commits in memory transaction", b)
		b.commitTxBothLayers()
		return nil
	}

	glog.V(2).Infof("%v commits persistent transaction", b)
	return b.replicate()
}

func (b *bee) AbortTx() error {
	dicts, msgs := b.currentState()
	if dicts.TxStatus() != state.TxOpen {
		return state.ErrNoTx
	}

	glog.V(2).Infof("%v aborts tx", b)
	err := dicts.AbortTx()
	b.resetTx(dicts, msgs)
	return err
}

func (b *bee) Snooze(d time.Duration) {
	panic(d)
}

func (b *bee) Save() ([]byte, error) {
	return b.stateL1.Save()
}

func (b *bee) Restore(buf []byte) error {
	return b.stateL1.Restore(buf)
}

func (b *bee) Apply(req interface{}) (interface{}, error) {
	b.Lock()
	defer b.Unlock()

	switch r := req.(type) {
	case commitTx:
		if b.txTerm < r.Term {
			b.txTerm = r.Term
		} else if r.Term < b.txTerm {
			return nil, ErrOldTx
		}

		glog.V(2).Infof("%v commits %v", b, r)
		leader := b.isLeader()

		if b.stateL2 != nil {
			b.stateL2 = nil
			glog.Errorf("%v has an L2 transaction", b)
		}

		if b.stateL1.TxStatus() == state.TxOpen {
			if !leader {
				glog.Errorf("%v is a follower and has an open transaction", b)
			}
			b.resetTx(b.stateL1, &b.msgBufL1)
		}

		if err := b.stateL1.Apply(r.Tx.Ops); err != nil {
			return nil, err
		}

		if leader && b.emitInRaft {
			for _, msg := range r.Tx.Msgs {
				msg.MsgFrom = b.beeID
				glog.V(2).Infof("%v emits %#v", b, msg)
			}
			b.throttle(r.Tx.Msgs)
		}
		return nil, nil

	case noOp:
		return nil, nil
	}
	glog.Errorf("%v cannot handle %v", b, req)
	return nil, ErrUnsupportedRequest
}

func (b *bee) ApplyConfChange(cc raftpb.ConfChange, gn raft.GroupNode) error {
	if gn.Data == nil {
		return nil
	}

	b.Lock()
	defer b.Unlock()

	col := b.beeColony
	bid := gn.Data.(uint64)

	switch cc.Type {
	case raftpb.ConfChangeAddNode:
		if col.Contains(bid) {
			return ErrDuplicateBee
		}
		col.AddFollower(bid)
	case raftpb.ConfChangeRemoveNode:
		if !col.Contains(bid) {
			return ErrNoSuchBee
		}
		if bid == b.beeID {
			// TODO(soheil): should we stop the bee here?
			glog.Fatalf("bee is alive but removed from raft")
		}
		if col.Leader == bid {
			// TODO(soheil): should we launch a goroutine to campaign here?
			col.Leader = 0
		} else {
			col.DelFollower(bid)
		}
	}
	b.beeColony = col
	return nil
}

// commitTx is a bee raft command that is applied when a transaction is
// commited.
type commitTx struct {
	Tx   tx
	Term uint64
}

func init() {
	gob.Register(commitTx{})
}
