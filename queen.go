package beehive

import (
	"errors"
	"fmt"
	"runtime/debug"
	"strconv"
	"sync"

	"github.com/kandoo/beehive/Godeps/_workspace/src/code.google.com/p/go.net/context"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/golang/glog"
	"github.com/kandoo/beehive/state"
)

// An applictaion's queen bee is the light weight thread that routes messags
// through the bees of that application.
type qee struct {
	sync.RWMutex

	hive *hive
	app  *app

	dataCh  *msgChannel
	ctrlCh  chan cmdAndChannel
	stopped bool

	state State

	bees map[uint64]*bee
}

func (q *qee) start() {
	q.stopped = false
	dataCh := q.dataCh.out()
	for !q.stopped {
		select {
		case d := <-dataCh:
			q.handleMsg(d)

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

func (q *qee) allocateNewBeeID() (BeeInfo, error) {
	res, err := q.hive.node.Process(context.TODO(), newBeeID{})
	if err != nil {
		return BeeInfo{}, err
	}
	id := res.(uint64)
	info := BeeInfo{
		ID:   id,
		Hive: q.hive.ID(),
		App:  q.app.Name(),
	}
	return info, nil
}

func (q *qee) newLocalBee(withInitColony bool) (*bee, error) {
	info, err := q.allocateNewBeeID()
	if err != nil {
		return nil, fmt.Errorf("%v cannot allocate a new bee ID: %v", q, err)
	}

	if _, ok := q.beeByID(info.ID); ok {
		return nil, fmt.Errorf("bee %v already exists", info.ID)
	}

	b := q.defaultLocalBee(info.ID)
	b.setState(q.app.newState())
	if withInitColony {
		c := Colony{Leader: info.ID}
		info.Colony = c
		b.beeColony = c
		b.becomeLeader()
	} else {
		b.becomeZombie()
	}
	if _, err := q.hive.node.Process(context.TODO(), addBee(info)); err != nil {
		return nil, err
	}

	q.addBee(b)
	go b.start()
	return b, nil
}

func (q *qee) newProxyBee(info BeeInfo) (*bee, error) {
	if q.isLocalBee(info) {
		return nil, errors.New("cannot create proxy for a local bee")
	}

	p, err := q.hive.newProxyToHive(info.Hive)
	if err != nil {
		return nil, err
	}
	b := q.defaultLocalBee(info.ID)
	b.becomeProxy(p)
	q.addBee(b)
	go b.start()
	return b, nil
}

func (q *qee) newDetachedBee(h DetachedHandler) (*bee, error) {
	info, err := q.allocateNewBeeID()
	if err != nil {
		return nil, fmt.Errorf("%v cannot allocate a new bee ID: %v", q, err)
	}
	info.Detached = true
	b := q.defaultLocalBee(info.ID)
	b.setState(q.app.newState())
	b.becomeDetached(h)
	if _, err := q.hive.node.Process(context.TODO(), addBee(info)); err != nil {
		return nil, err
	}
	q.addBee(b)
	go b.startDetached(h)
	return b, nil
}

func (q *qee) processCmd(data interface{}) (interface{}, error) {
	ch := make(chan cmdResult)
	q.ctrlCh <- newCmdAndChannel(data, q.app.Name(), 0, ch)
	return (<-ch).get()
}

func (q *qee) sendCmdToBee(bid uint64, data interface{}) (interface{}, error) {
	if b, ok := q.beeByID(bid); ok {
		ch := make(chan cmdResult)
		b.enqueCmd(newCmdAndChannel(data, q.app.Name(), bid, ch))
		return (<-ch).get()
	}

	info, err := q.hive.registry.bee(bid)
	if err != nil {
		return nil, err
	}

	if q.isLocalBee(info) {
		glog.Fatalf("%v cannot find local bee %v", q, bid)
	}

	prx, err := q.hive.newProxyToHive(info.Hive)
	if err != nil {
		return nil, err
	}

	return prx.sendCmd(&cmd{
		To:   bid,
		App:  q.app.Name(),
		Data: data,
	})
}

func (q *qee) stopBees() {
	q.RLock()
	defer q.RUnlock()
	stopCh := make(chan cmdResult)
	stopCmd := newCmdAndChannel(cmdStop{}, q.app.Name(), 0, stopCh)
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
	if cc.cmd.To != 0 {
		if b, ok := q.beeByID(cc.cmd.To); ok {
			b.enqueCmd(cc)
			return
		}

		// If the bee is a proxy we should try to relay the message.
		if info, err := q.hive.registry.bee(cc.cmd.To); err == nil &&
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
				Err: fmt.Errorf("%v cannot find bee %v", q, cc.cmd.To),
			}
		}
		return
	}

	glog.V(2).Infof("%v handles command %#v", q, cc.cmd.Data)

	switch cmd := cc.cmd.Data.(type) {
	case cmdStop:
		q.stopped = true
		glog.V(3).Infof("stopping bees of %p", q)
		q.stopBees()
		cc.ch <- cmdResult{}
		return

	case cmdFindBee:
		id := cmd.ID
		r, ok := q.beeByID(id)
		if ok {
			cc.ch <- cmdResult{Data: r}
			return
		}

		err := fmt.Errorf("%v cannot find bee %v", q, id)
		cc.ch <- cmdResult{Err: err}
		return

	case cmdCreateBee:
		b, err := q.newLocalBee(false)
		if err != nil {
			cc.ch <- cmdResult{Err: err}
			glog.Error(err)
			return
		}
		cc.ch <- cmdResult{Data: b.ID()}
		glog.V(2).Infof("created a new local bee %v", b)
		return

	case cmdReloadBee:
		_, err := q.reloadBee(cmd.ID, cmd.Colony)
		cc.ch <- cmdResult{Err: err}
		return

	case cmdStartDetached:
		b, err := q.newDetachedBee(cmd.Handler)
		if cc.ch != nil {
			cc.ch <- cmdResult{
				Data: b.ID(),
				Err:  err,
			}
		}

	case cmdMigrate:
		bid, err := q.migrate(cmd.Bee, cmd.To)
		cc.ch <- cmdResult{
			Data: bid,
			Err:  err,
		}

	default:
		if cc.ch != nil {
			glog.Errorf("Unknown bee command %#v", cmd)
			cc.ch <- cmdResult{
				Err: fmt.Errorf("Unknown bee command %#v", cmd),
			}
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

func (q *qee) handleMsg(mh msgAndHandler) {
	if mh.msg.IsUnicast() {
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
		return
	}

	glog.V(2).Infof("%v broadcasts message %v", q, mh.msg)

	cells := q.invokeMap(mh)
	if cells == nil {
		glog.V(2).Infof("%v drops message %v", q, mh.msg)
		return
	}

	if cells.LocalBroadcast() {
		q.RLock()
		defer q.RUnlock()
		glog.V(2).Infof("%v sends a message to all local bees: %v", q, mh.msg)
		for id, b := range q.bees {
			if b.detached || b.proxy {
				continue
			}
			if b.colony().Leader != id {
				continue
			}
			b.enqueMsg(mh)
		}
		return
	}

	b, err := q.beeByCells(cells)
	if err != nil {
		if b, err = q.placeBee(cells); err != nil {
			glog.Fatalf("%v cannot place a new bee %v", q, err)
		}

		info := BeeInfo{
			Hive:   q.hive.ID(),
			App:    q.app.Name(),
			ID:     b.ID(),
			Colony: Colony{Leader: b.ID()},
		}

		if info, err = q.lock(info, cells); err != nil {
			glog.Fatalf("error in locking the cells: %v", err)
		}

		if info.ID == b.ID() {
			b.processCmd(cmdAddMappedCells{Cells: cells})
		} else {
			b, err = q.beeByCells(cells)
			if err != nil {
				glog.Fatalf("neither can lock a cell nor can find its bee")
			}
		}
	}

	glog.V(2).Infof("message sent to bee %v: %v", b, mh.msg)
	b.enqueMsg(mh)
}

func (q *qee) placeBee(cells MappedCells) (*bee, error) {
	if q.app.placement == nil || q.app.placement == PlacementMethod(nil) {
		return q.newLocalBee(true)
	}

	h := q.app.placement.Place(cells, q.hive, q.hive.registry.hives())
	if h.ID == q.hive.ID() {
		return q.newLocalBee(true)
	}

	var col Colony
	var bi BeeInfo
	var b *bee
	cmd := &cmd{
		App:  q.app.Name(),
		Data: cmdCreateBee{},
	}
	p := newProxy(q.hive.client, h.Addr)
	res, err := p.sendCmd(cmd)
	if err != nil {
		goto fallback
	}
	col.Leader = res.(uint64)

	cmd.To = col.Leader
	cmd.Data = cmdJoinColony{
		Colony: col,
	}
	if _, err = p.sendCmd(cmd); err != nil {
		goto fallback
	}

	bi = BeeInfo{
		ID:     col.Leader,
		Hive:   h.ID,
		App:    q.app.Name(),
		Colony: col,
	}
	b, err = q.newProxyBee(bi)
	if err != nil {
		goto fallback
	}
	q.addBee(b)
	return b, nil

fallback:
	glog.Errorf("%v cannot create a new bee on %v. will place locally: %v", q,
		h.ID, err)
	return q.newLocalBee(true)
}

func (q *qee) lock(b BeeInfo, cells MappedCells) (BeeInfo, error) {
	res, err := q.hive.node.Process(context.TODO(), LockMappedCell{
		Colony: b.Colony,
		App:    b.App,
		Cells:  cells,
	})
	if err != nil {
		return b, err
	}

	col := res.(Colony)
	if col.Leader == b.ID {
		return b, nil
	}

	info, err := q.hive.registry.bee(col.Leader)
	if err != nil {
		// TODO(soheil): Should we be graceful here?
		glog.Fatalf("cannot find bee %v: %v", col.Leader, err)
	}
	return info, nil
}

func (q *qee) beeByCells(cells MappedCells) (*bee, error) {
	info, all, err := q.hive.registry.beeForCells(q.app.Name(), cells)
	if err != nil {
		return nil, err
	}

	if !all {
		info, err = q.lock(info, cells)
		if err != nil {
			return nil, err
		}
		// TODO(soheil): Should we check incosistencies?
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

	var prx *proxy
	var res interface{}

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

	if prx, err = q.hive.newProxyToHive(to); err != nil {
		return Nil, err
	}

	res, err = prx.sendCmd(&cmd{
		App:  q.app.Name(),
		Data: cmdCreateBee{},
	})
	if err != nil {
		return Nil, err
	}

	newb = res.(uint64)
	// TODO(soheil): we need to do this for persitent apps with a replication
	// factor of 1 as well.
	if !q.app.persistent() {
		_, err = prx.sendCmd(&cmd{
			To:   newb,
			App:  q.app.Name(),
			Data: cmdJoinColony{Colony: Colony{Leader: newb}},
		})
		if err != nil {
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
	return &bee{
		qee:       q,
		beeID:     id,
		dataCh:    newMsgChannel(q.hive.config.DataChBufSize),
		ctrlCh:    make(chan cmdAndChannel, cap(q.ctrlCh)),
		hive:      q.hive,
		app:       q.app,
		peers:     make(map[uint64]*proxy),
		batchSize: q.hive.config.BatchSize,
	}
}

func (q *qee) enqueMsg(mh msgAndHandler) {
	q.dataCh.in() <- mh
}
