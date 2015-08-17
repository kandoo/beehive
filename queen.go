package beehive

import (
	"errors"
	"fmt"
	"runtime/debug"
	"strconv"
	"sync"

	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/golang/glog"
	"github.com/kandoo/beehive/Godeps/_workspace/src/golang.org/x/net/context"
	"github.com/kandoo/beehive/bucket"
	"github.com/kandoo/beehive/state"
)

// An applictaion's queen bee is the light weight thread that routes messags
// through the bees of that application.
type qee struct {
	sync.RWMutex

	hive *hive
	app  *app

	dataCh      *msgChannel
	ctrlCh      chan cmdAndChannel
	placementCh chan placementRes
	stopped     bool

	state *state.Transactional

	bees         map[uint64]*bee
	pendingCells map[CellKey]*pendingCells

	maxID  uint64
	nextID uint64
}

func (q *qee) start() {
	batch := make([]msgAndHandler, 0, q.hive.config.BatchSize)
	q.stopped = false
	dataCh := q.dataCh.out()
	for !q.stopped {
		select {
		case d := <-dataCh:
			batch = append(batch, d)
			l := len(dataCh)
			if cap(batch)-1 < l {
				l = cap(batch) - 1
			}
			for i := 0; i < l; i++ {
				batch = append(batch, <-dataCh)
			}
			q.handleMsgs(batch)
			batch = batch[0:0]

		case p := <-q.placementCh:
			// TODO(soheil): maybe batch.
			q.handlePlacementRes(p)

		case c := <-q.ctrlCh:
			q.handleCmd(c)
		}
	}
}

func (q *qee) String() string {
	return fmt.Sprintf("%d/%s/Q", q.hive.ID(), q.app.Name())
}

func (q *qee) Dict(n string) state.Dict {
	return q.state.Dict(n)
}

func (q *qee) Hive() Hive {
	return q.hive
}

func (q *qee) LocalMappedCells() MappedCells {
	return MappedCells{{"__nil_dict__", strconv.FormatUint(q.hive.ID(), 10)}}
}

func (q *qee) App() string {
	return q.app.Name()
}

func (q *qee) Sync(ctx context.Context, req interface{}) (res interface{},
	err error) {

	return q.hive.Sync(ctx, req)
}

func (q *qee) beeByID(id uint64) (b *bee, ok bool) {
	q.RLock()
	b, ok = q.bees[id]
	q.RUnlock()
	return b, ok
}

func (q *qee) addBee(b *bee) {
	q.Lock()
	// TODO(soheil): should we validate the map first?
	q.bees[b.ID()] = b
	q.Unlock()
}

func (q *qee) allocateBeeID() error {
	a := allocateBeeIDs{
		Len: q.hive.config.BatchSize,
	}
	res, err := q.hive.node.ProposeRetry(hiveGroup, a,
		q.hive.config.RaftElectTimeout(), -1)
	if err != nil {
		return err
	}
	ares := res.(allocateBeeIDResult)
	q.nextID = ares.From
	q.maxID = ares.To
	return nil
}

func (q *qee) newBeeID() (bid uint64, err error) {
	if q.maxID == 0 || q.nextID == q.maxID {
		if err := q.allocateBeeID(); err != nil {
			return 0, err
		}
	}

	bid = q.nextID
	q.nextID++
	return
}

func (q *qee) newLocalBee(withColony bool) (*bee, error) {
	id, err := q.newBeeID()
	if err != nil {
		return nil, fmt.Errorf("%v cannot allocate a new bee ID: %v", q, err)
	}

	if _, ok := q.beeByID(id); ok {
		return nil, fmt.Errorf("bee %v already exists", id)
	}

	info := q.defaultBeeInfo(id, false, withColony)
	if err := q.registerBee(info); err != nil {
		return nil, err
	}

	return q.newLocalBeeWithID(id, withColony)
}

func (q *qee) defaultBeeInfo(id uint64, detached bool, initColony bool) (
	info BeeInfo) {

	info.ID = id
	info.App = q.App()
	info.Hive = q.hive.ID()
	if initColony {
		info.Colony = q.defaultColony(id)
	}
	info.Detached = detached
	return
}

func (q *qee) defaultColony(bee uint64) Colony {
	return Colony{
		ID:     bee,
		Leader: bee,
	}
}

func (q *qee) newLocalBeeWithID(id uint64, withColony bool) (*bee, error) {
	b := q.defaultLocalBee(id)
	b.setState(q.app.newState())

	if withColony {
		b.beeColony = q.defaultColony(id)
		b.becomeLeader()
	} else {
		b.becomeZombie()
	}

	q.addBee(b)
	go b.start()
	return b, nil
}

func (q *qee) newProxyBee(info BeeInfo) (*bee, error) {
	if q.isLocalBee(info) {
		return nil, errors.New("cannot create proxy for a local bee")
	}

	b := q.defaultLocalBee(info.ID)
	b.becomeProxy()
	q.addBee(b)
	go b.start()
	return b, nil
}

func (q *qee) newDetachedBee(h DetachedHandler) (*bee, error) {
	id, err := q.newBeeID()
	if err != nil {
		return nil, fmt.Errorf("%v cannot allocate a new bee ID: %v", q, err)
	}
	b := q.defaultLocalBee(id)
	b.setState(q.app.newState())
	b.becomeDetached(h)

	if err := q.registerBee(q.defaultBeeInfo(id, true, false)); err != nil {
		return nil, err
	}

	q.addBee(b)

	go b.startDetached(h)
	return b, nil
}

func (q *qee) registerBee(info BeeInfo) error {
	// TODO(soheil): we should not block on this.
	_, err := q.hive.node.ProposeRetry(hiveGroup, addBee(info),
		q.hive.config.RaftElectTimeout(), -1)
	return err
}

func (q *qee) processCmd(data interface{}) (interface{}, error) {
	ch := make(chan cmdResult)
	q.ctrlCh <- newCmdAndChannel(data, q.hive.ID(), q.app.Name(), 0, ch)
	return (<-ch).get()
}

func (q *qee) sendCmdToBee(bid uint64, data interface{}) (interface{}, error) {
	if b, ok := q.beeByID(bid); ok {
		ch := make(chan cmdResult)
		b.enqueCmd(newCmdAndChannel(data, q.hive.ID(), q.app.Name(), bid, ch))
		return (<-ch).get()
	}

	info, err := q.hive.registry.bee(bid)
	if err != nil {
		return nil, err
	}

	if q.isLocalBee(info) {
		glog.Fatalf("%v cannot find local bee %v", q, bid)
	}

	cmd := cmd{
		Hive: info.Hive,
		App:  info.App,
		Bee:  info.ID,
		Data: data,
	}
	return q.hive.client.sendCmd(cmd)
}

func (q *qee) stopBees() {
	q.RLock()
	defer q.RUnlock()
	stopCh := make(chan cmdResult)
	stopCmd := newCmdAndChannel(cmdStop{}, q.hive.ID(), q.app.Name(), 0, stopCh)
	for _, b := range q.bees {
		glog.V(2).Infof("%v is stopping %v", q, b)
		b.enqueCmd(stopCmd)

		_, err := (<-stopCh).get()
		if err != nil {
			glog.Errorf("%v cannot stop %v: %v", q, b, err)
		}
	}
}

func (q *qee) handleCmd(cc cmdAndChannel) {
	if cc.cmd.Bee != Nil {
		if b, ok := q.beeByID(cc.cmd.Bee); ok {
			b.enqueCmd(cc)
			return
		}

		// If the bee is a proxy we should try to relay the message.
		if info, err := q.hive.registry.bee(cc.cmd.Bee); err == nil &&
			!q.isLocalBee(info) {

			if b, err := q.newProxyBee(info); err == nil {
				b.enqueCmd(cc)
				return
			} else {
				glog.Warningf("cannot create proxy to %#v", info)
			}
		}

		if cc.ch != nil {
			cc.ch <- cmdResult{
				Err: fmt.Errorf("%v cannot find bee %v", q, cc.cmd.Bee),
			}
		}
		return
	}

	glog.V(2).Infof("%v handles command %#v", q, cc.cmd.Data)
	var err error
	var res interface{}
	switch cmd := cc.cmd.Data.(type) {
	case cmdStop:
		q.stopped = true
		glog.V(3).Infof("stopping bees of %p", q)
		q.stopBees()

	case cmdFindBee:
		id := cmd.ID
		r, ok := q.beeByID(id)
		if !ok {
			err = fmt.Errorf("%v cannot find bee %v", q, id)
			break
		}
		res = r

	case cmdCreateBee:
		var b *bee
		b, err = q.newLocalBee(false)
		if err != nil {
			break
		}
		res = b.ID()
		glog.V(2).Infof("created a new local bee %v", b)

	case cmdReloadBee:
		_, err = q.reloadBee(cmd.ID, cmd.Colony)

	case cmdStartDetached:
		var b *bee
		b, err = q.newDetachedBee(cmd.Handler)
		if b != nil {
			res = b.ID()
		}

	case cmdMigrate:
		res, err = q.migrate(cmd.Bee, cmd.To)

	default:
		err = fmt.Errorf("unknown queen bee command %#v", cmd)
	}

	if err != nil {
		glog.Errorf("%v cannot handle %v: %v", q, cc.cmd, err)
	}

	if cc.ch != nil {
		cc.ch <- cmdResult{
			Err:  err,
			Data: res,
		}
	}
}

func (q *qee) reloadBee(id uint64, col Colony) (*bee, error) {
	info, err := q.hive.bee(id)
	if err != nil {
		return nil, err
	}
	b := q.defaultLocalBee(id)
	b.setState(q.app.newState())
	b.setColony(info.Colony)
	if b.isLeader() {
		b.becomeLeader()
	} else {
		b.becomeFollower()
	}
	q.addBee(b)
	glog.V(2).Infof("%v reloads %v", q, b)
	go b.start()
	return b, nil
}

func (q *qee) invokeMap(mh msgAndHandler) (ms MappedCells) {
	defer func() {
		if r := recover(); r != nil {
			glog.Errorf("error in map of %s: %v\n%s", q.app.Name(), r,
				string(debug.Stack()))
			ms = nil
		}
	}()

	glog.V(2).Infof("%v invokes map for %v", q, mh.msg)
	return mh.handler.Map(mh.msg, q)
}

func (q *qee) isDetached(id uint64) bool {
	b, err := q.hive.registry.bee(id)
	return err == nil && b.Detached
}

func (q *qee) handleUnicastMsg(mh msgAndHandler) {
	glog.V(2).Infof("unicast msg: %v", mh.msg)
	b, ok := q.beeByID(mh.msg.To())
	if !ok {
		info, err := q.hive.registry.bee(mh.msg.To())
		if err != nil {
			glog.Errorf("cannot find bee %v", mh.msg.To())
		}

		if q.isLocalBee(info) {
			glog.Fatalf("%v cannot find local bee %v", q, mh.msg.To())
		}

		if b, ok = q.beeByID(info.ID); !ok {
			if b, err = q.newProxyBee(info); err != nil {
				glog.Errorf("%v cannnot find remote bee %v", q, mh.msg.To())
				return
			}
		}
	}

	if mh.handler == nil && !b.detached && !b.proxy {
		glog.Fatalf("handler is nil for message %v", mh.msg)
	}

	b.enqueMsg(mh)
}

func (q *qee) handleLocalBcast(mh msgAndHandler) {
	glog.V(2).Infof("%v sends a message to all local bees: %v", q, mh.msg)

	q.RLock()
	for id, b := range q.bees {
		if b.detached || b.proxy {
			continue
		}
		if b.colony().Leader != id {
			continue
		}
		b.enqueMsg(mh)
	}
	q.RUnlock()
}

type placementRes struct {
	hive   uint64
	colony Colony
	pCells *pendingCells
}

type pendingCells struct {
	visited bool

	bee   *bee
	beeID uint64

	cells map[CellKey]struct{}
	msgs  []msgAndHandler
}

func newBeeCellMsgs() *pendingCells {
	return &pendingCells{
		cells: make(map[CellKey]struct{}),
	}
}

func (pc *pendingCells) MappedCells() (mapped MappedCells) {
	for c := range pc.cells {
		mapped = append(mapped, c)
	}
	return
}

func (q *qee) addToPendings(pc *pendingCells) {
	for c := range pc.cells {
		q.pendingCells[c] = pc
	}
}

func (q *qee) queueIfPending(cells MappedCells, mh msgAndHandler) (ok bool) {
	if len(q.pendingCells) == 0 {
		return false
	}

	var pc *pendingCells
	for _, c := range cells {
		if pc, ok = q.pendingCells[c]; ok {
			pc.msgs = append(pc.msgs, mh)
			break
		}
	}

	if !ok || len(cells) == 1 {
		return ok
	}

	for _, c := range cells {
		q.pendingCells[c] = pc
	}
	return true
}

func (q *qee) removePending(pc *pendingCells) {
	for cell := range pc.cells {
		delete(q.pendingCells, cell)
	}
}

func (q *qee) handlePlacementRes(res placementRes) error {
	defer q.removePending(res.pCells)

	if res.colony.IsNil() {
		b, err := q.newLocalBee(true)
		if err != nil {
			return err
		}

		lock := lockMappedCell{
			Colony: b.colony(),
			App:    q.app.Name(),
			Cells:  res.pCells.MappedCells(),
		}

		lockRes, err := q.hive.node.ProposeRetry(hiveGroup, lock,
			q.hive.config.RaftElectTimeout(), -1)
		if err != nil {
			return err
		}

		col := lockRes.(Colony)
		if col.Leader == b.ID() {
			b.processCmd(cmdAddMappedCells{Cells: lock.Cells})
		} else {
			var err error
			if b, err = q.beeByCells(lock.Cells); err != nil {
				return err
			}
		}

		for _, mh := range res.pCells.msgs {
			b.enqueMsg(mh)
		}
		return nil
	}

	bi := BeeInfo{
		ID:     res.colony.Leader,
		Hive:   res.hive,
		App:    q.app.Name(),
		Colony: res.colony,
	}
	b, err := q.newProxyBee(bi)
	if err != nil {
		return err
	}
	q.addBee(b)
	for _, mh := range res.pCells.msgs {
		b.enqueMsg(mh)
	}
	return nil
}

func (q *qee) handleMsgs(mhs []msgAndHandler) {
	pendingC := make(map[CellKey]*pendingCells)

	for i := range mhs {
		mh := mhs[i]
		if mh.msg.IsUnicast() {
			q.handleUnicastMsg(mh)
			continue
		}

		glog.V(2).Infof("%v broadcasts message %v", q, mh.msg)

		cells := q.invokeMap(mh)
		if cells == nil {
			glog.V(2).Infof("%v drops message %v", q, mh.msg)
			continue
		}

		if cells.LocalBroadcast() {
			q.handleLocalBcast(mh)
			continue
		}

		if q.queueIfPending(cells, mh) {
			continue
		}

		b, err := q.beeByCells(cells)
		if err == nil {
			b.enqueMsg(mh)
			continue
		}

		var bcm *pendingCells
		ok := false
		for _, c := range cells {
			if bcm, ok = pendingC[c]; ok {
				break
			}
		}

		if !ok {
			bcm = newBeeCellMsgs()
		}

		for _, c := range cells {
			// FIXME(soheil): what if map returns conflicting cells.
			pendingC[c] = bcm
			bcm.cells[c] = struct{}{}
		}

		bcm.msgs = append(bcm.msgs, mhs[i])
	}

	if len(pendingC) == 0 {
		return
	}

	var lockBatch batchReq
	for _, pc := range pendingC {
		if pc.visited {
			continue
		}

		pc.visited = true

		// TODO(soheil): we should do this in parallel.
		mapped := pc.MappedCells()
		if mapped == nil {
			panic(mapped)
		}
		hive := q.placeBee(mapped)

		if hive != q.hive.ID() {
			q.addToPendings(pc)
			go q.newRemoteBee(pc, hive)
			continue
		}

		var err error
		pc.beeID, err = q.newBeeID()
		if err != nil {
			// TODO(soheil): this shouldn't be fatal.
			glog.Fatalf("%v cannot allocate a bee ID %v", q, err)
		}
		lockBatch.addReq(addBee(q.defaultBeeInfo(pc.beeID, false, true)))
		lockBatch.addReq(lockMappedCell{
			Colony: q.defaultColony(pc.beeID),
			App:    q.app.Name(),
			Cells:  mapped,
		})
	}

	lockRes, err := q.hive.node.ProposeRetry(hiveGroup, lockBatch,
		q.hive.config.RaftElectTimeout(), -1)
	if err != nil {
		glog.Fatalf("error in lock cells: %v", err)
	}

	var wg sync.WaitGroup
	for i, r := range lockRes.(batchRes) {
		if !r.Err.IsNil() {
			glog.Fatalf("cannot lock the cells TODO: %v", r.Err)
		}

		lock, ok := lockBatch.Reqs[i].(lockMappedCell)
		if !ok {
			// We can simply ignore add bee requests in the batch.
			continue
		}

		wg.Add(1)
		go func(res interface{}, lock lockMappedCell) {
			cells := lock.Cells
			pc := pendingC[cells[0]]
			if res.(Colony).Leader == lock.Colony.Leader {
				if pc.bee == nil {
					var err error
					if pc.bee, err = q.newLocalBeeWithID(pc.beeID, true); err != nil {
						glog.Fatalf("%v cannot create local bee %v", err)
					}
				}
				pc.bee.processCmd(cmdAddMappedCells{Cells: cells})
			} else {
				// TODO(soheil): maybe, we can find by id.
				var err error
				if pc.bee, err = q.beeByCells(cells); err != nil {
					glog.Fatalf("neither can lock a cell nor can find its bee")
				}
			}

			for _, mh := range pc.msgs {
				glog.V(2).Infof("%v enques message to bee %v: %v", q, pc.bee, mh.msg)
				pc.bee.enqueMsg(mh)
			}
			wg.Done()
		}(r.Res, lock)
	}

	wg.Wait()
}

func (q *qee) newRemoteBee(pc *pendingCells, hive uint64) {
	var col Colony
	cmd := cmd{
		Hive: hive,
		App:  q.app.Name(),
		Data: cmdCreateBee{},
	}
	res, err := q.hive.client.sendCmd(cmd)
	if err != nil {
		q.placementCh <- placementRes{pCells: pc}
		goto fallback
	}
	col.Leader = res.(uint64)

	cmd.Bee = col.Leader
	cmd.Data = cmdJoinColony{
		Colony: col,
	}
	if _, err = q.hive.client.sendCmd(cmd); err != nil {
		goto fallback
	}

	q.placementCh <- placementRes{
		hive:   hive,
		colony: col,
		pCells: pc,
	}
	return

fallback:
	glog.Errorf("%v cannot create a new bee on %v. will place locally: %v", q,
		hive, err)
	q.placementCh <- placementRes{pCells: pc}
}

func (q *qee) placeBee(cells MappedCells) (hiveID uint64) {
	if q.app.placement == nil || q.app.placement == PlacementMethod(nil) {
		return q.hive.ID()
	}

	defer func() {
		if r := recover(); r != nil {
			hiveID = q.hive.ID()
		}
	}()

	h := q.app.placement.Place(cells, q.hive, q.hive.registry.hives())
	return h.ID
}

func (q *qee) beeByCells(cells MappedCells) (*bee, error) {
	info, all, err := q.hive.registry.beeForCells(q.app.Name(), cells)
	if err != nil {
		return nil, err
	}

	if !all {
		// TODO(soheil): should we check incosistencies?
		lock := lockMappedCell{
			Colony: info.Colony,
			App:    q.app.Name(),
			Cells:  cells,
		}
		if _, err := q.hive.node.ProposeRetry(hiveGroup, lock,
			q.hive.config.RaftElectTimeout(), -1); err != nil {

			return nil, err
		}
		// TODO(soheil): maybe check whether the leader has changed?
	}

	b, ok := q.beeByID(info.ID)
	if ok {
		return b, nil
	}

	if q.isLocalBee(info) {
		glog.Fatalf("%v cannot find local bee %v", q, info.ID)
	}

	b, err = q.newProxyBee(info)
	if b == nil || err != nil {
		glog.Errorf("%v cannot create proxy to %v", q, info.ID)
	}
	return b, err
}

func (q *qee) migrate(bid uint64, to uint64) (newb uint64, err error) {
	if q.isDetached(bid) {
		return Nil, fmt.Errorf("cannot migrate a detached: %#v", bid)
	}

	glog.V(2).Infof("%v starts to migrate %v to %v", q, bid, to)

	var r interface{}
	var c cmd

	oldb, ok := q.beeByID(bid)
	if !ok {
		return Nil, fmt.Errorf("%v cannot find %v", q, bid)
	}

	if oldb.detached || oldb.proxy {
		return Nil, fmt.Errorf("%v cannot migrate nonlocal bee %v", q, bid)
	}

	oldc := oldb.colony()
	for _, f := range oldc.Followers {
		if info, err := q.hive.bee(f); err == nil && info.Hive == to {
			glog.V(2).Infof("%v found follower %v on %v, will hand off", q, f, to)
			newb = f
			goto handoff
		}
	}

	c = cmd{
		Hive: to,
		App:  q.app.Name(),
		Data: cmdCreateBee{},
	}
	r, err = q.hive.client.sendCmd(c)
	if err != nil {
		return Nil, err
	}

	newb = r.(uint64)
	// TODO(soheil): we need to do this for persitent apps with a replication
	// factor of 1 as well.
	if !q.app.persistent() {
		c = cmd{
			Hive: to,
			App:  q.app.Name(),
			Bee:  newb,
			Data: cmdJoinColony{Colony: Colony{Leader: newb}},
		}
		if _, err = q.hive.client.sendCmd(c); err != nil {
			return Nil, err
		}
		goto handoff
	}

	if _, err = oldb.processCmd(cmdAddFollower{Hive: to, Bee: newb}); err != nil {
		// TODO(soheil): Maybe stop newb?
		return Nil, err
	}

handoff:
	if err = q.hive.raftBarrier(); err != nil {
		return Nil, err
	}
	if _, err = oldb.processCmd(cmdHandoff{To: newb}); err != nil {
		glog.Errorf("%v cannot handoff to %v: %v", oldb, newb, err)
		return Nil, err
	}
	return newb, nil
}

func (q *qee) isLocalBee(info BeeInfo) bool {
	return q.hive.ID() == info.Hive
}

func (q *qee) defaultLocalBee(id uint64) *bee {
	var inb *bucket.Bucket
	if q.app.rate.inRate == 0 {
		inb = bucket.New(bucket.Unlimited, 0, bucket.DefaultResolution)
	} else {
		inb = bucket.New(q.app.rate.inRate, q.app.rate.inMaxTokens,
			bucket.DefaultResolution)
	}

	var outb *bucket.Bucket
	if q.app.rate.outRate == 0 {
		outb = bucket.New(bucket.Unlimited, 0, bucket.DefaultResolution)
	} else {
		outb = bucket.New(q.app.rate.outRate, q.app.rate.outMaxTokens,
			bucket.DefaultResolution)
	}

	var batch uint
	if uint(inb.Max()) < q.hive.config.BatchSize {
		batch = uint(inb.Max())
	} else {
		batch = q.hive.config.BatchSize
	}

	return &bee{
		qee:       q,
		beeID:     id,
		dataCh:    newMsgChannel(q.hive.config.DataChBufSize),
		ctrlCh:    make(chan cmdAndChannel, cap(q.ctrlCh)),
		hive:      q.hive,
		app:       q.app,
		batchSize: batch,
		inBucket:  inb,
		outBucket: outb,
	}
}

func (q *qee) enqueMsg(mh msgAndHandler) {
	q.dataCh.in() <- mh
}
