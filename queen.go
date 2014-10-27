package beehive

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

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

	dataCh  chan msgAndHandler
	ctrlCh  chan cmdAndChannel
	stopped bool

	state State

	bees map[uint64]bee
}

func (q *qee) start() {
	q.stopped = false
	for !q.stopped {
		select {
		case d := <-q.dataCh:
			q.handleMsg(d)

		case c := <-q.ctrlCh:
			q.handleCmd(c)
		}
	}
}

func (q *qee) String() string {
	return fmt.Sprintf("%d/%s/Q", q.hive.ID(), q.app.Name())
}

func (q *qee) State() State {
	return q.state
}

func (q *qee) Dict(n string) state.Dict {
	return q.State().Dict(n)
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

func (q *qee) beeByID(id uint64) (b bee, ok bool) {
	q.RLock()
	defer q.RUnlock()

	b, ok = q.bees[id]
	return b, ok
}

func (q *qee) addBee(b bee) {
	q.Lock()
	defer q.Unlock()

	// TODO(soheil): should we validate the map first?
	q.bees[b.ID()] = b
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

func (q *qee) newLocalBee(withInitColony bool) (*localBee, error) {
	info, err := q.allocateNewBeeID()
	if err != nil {
		return nil, fmt.Errorf("%v cannot allocate a new bee ID: %v", q, err)
	}

	if _, ok := q.beeByID(info.ID); ok {
		return nil, fmt.Errorf("bee %v already exists", info.ID)
	}

	b := q.defaultLocalBee(info.ID, false)
	if withInitColony {
		c := Colony{Leader: info.ID}
		info.Colony = c
		b.beeColony = c
	}
	if _, err := q.hive.node.Process(context.TODO(), addBee(info)); err != nil {
		return nil, err
	}

	q.addBee(&b)
	go b.start()
	return &b, nil
}

func (q *qee) newProxyBee(info BeeInfo) (*proxyBee, error) {
	if q.isLocalBee(info) {
		return nil, errors.New("cannot create proxy for a local bee")
	}

	p, err := q.hive.newProxy(info.Hive)
	if err != nil {
		return nil, err
	}

	b := &proxyBee{
		localBee: q.defaultLocalBee(info.ID, false),
		proxy:    p,
	}
	q.addBee(b)
	go b.start()
	return b, nil
}

func (q *qee) newDetachedBee(h DetachedHandler) (*detachedBee, error) {
	info, err := q.allocateNewBeeID()
	if err != nil {
		return nil, fmt.Errorf("%v cannot allocate a new bee ID: %v", q, err)
	}
	info.Detached = true
	glog.V(2).Infof("%v starts detached bee %v for app %v", q, info.ID, info.App)
	d := &detachedBee{
		localBee: q.defaultLocalBee(info.ID, true),
		h:        h,
	}
	if _, err := q.hive.node.Process(context.TODO(), addBee(info)); err != nil {
		return nil, err
	}
	q.addBee(d)
	go d.start()
	return d, nil
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

	prx, err := q.hive.newProxy(info.Hive)
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
		return

	default:
		if cc.ch != nil {
			glog.Errorf("Unknown bee command %v", cmd)
			cc.ch <- cmdResult{
				Err: fmt.Errorf("Unknown bee command %v", cmd),
			}
		}
	}
}

func (q *qee) reloadBee(id uint64, col Colony) (bee, error) {
	info, err := q.hive.bee(id)
	if err != nil {
		return nil, err
	}
	b := q.defaultLocalBee(id, false)
	b.setColony(info.Colony)
	q.addBee(&b)
	go b.start()
	return &b, nil
}

func (q *qee) invokeMap(mh msgAndHandler) (ms MappedCells) {
	defer func() {
		if r := recover(); r != nil {
			glog.Errorf("error in map of %s: %v", q.app.Name(), r)
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

func (q *qee) isDetachedBee(b bee) bool {
	_, ok := b.(*detachedBee)
	return ok
}

func (q *qee) isProxyBee(b bee) bool {
	_, ok := b.(*proxyBee)
	return ok
}

func (q *qee) handleMsg(mh msgAndHandler) {
	if mh.msg.IsUnicast() {
		glog.V(2).Infof("unicast msg: %v", mh.msg)
		bee, ok := q.beeByID(mh.msg.To())
		if !ok {
			info, err := q.hive.registry.bee(mh.msg.To())
			if err != nil {
				glog.Errorf("cannot find bee %v", mh.msg.To())
			}

			if q.isLocalBee(info) {
				glog.Fatalf("%v cannot find a local bee %v", q, mh.msg.To())
			}

			if bee, ok = q.beeByID(info.ID); !ok {
				glog.Errorf("%v cannnot find the remote bee %v", q, mh.msg.To())
				return
			}
		}

		if mh.handler == nil && !q.isDetachedBee(bee) && !q.isProxyBee(bee) {
			glog.Fatalf("handler is nil for message %v", mh.msg)
		}

		bee.enqueMsg(mh)
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
		for id, bee := range q.bees {
			if _, ok := bee.(*localBee); !ok {
				continue
			}
			if bee.colony().Leader != id {
				continue
			}
			bee.enqueMsg(mh)
		}
		return
	}

	bee, err := q.beeByCells(cells)
	if err != nil {
		lb, err := q.newLocalBee(true)
		if err != nil {
			glog.Fatal(err)
		}

		info := BeeInfo{
			Hive:   q.hive.ID(),
			App:    q.app.Name(),
			ID:     lb.ID(),
			Colony: lb.colony().DeepCopy(),
		}

		if info, err = q.lock(info, cells); err != nil {
			glog.Fatalf("error in locking the cells: %v", err)
		}

		if info.ID == lb.ID() {
			lb.addMappedCells(cells)
			bee = lb
		} else {
			bee, err = q.beeByCells(cells)
			if err != nil {
				glog.Fatalf("neither can lock a cell nor can find its bee")
			}
		}
	}

	glog.V(2).Infof("message sent to bee %v: %v", bee, mh.msg)
	bee.enqueMsg(mh)
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

func (q *qee) beeByCells(cells MappedCells) (bee, error) {
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

	var prx proxy
	var res interface{}

	b, ok := q.beeByID(bid)
	if !ok {
		return Nil, fmt.Errorf("%v cannot find %v", q, bid)
	}

	oldb, ok := b.(*localBee)
	if !ok {
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

	if prx, err = q.hive.newProxy(to); err != nil {
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
	if _, err = oldb.processCmd(cmdAddFollower{Hive: to, Bee: newb}); err != nil {
		// TODO(soheil): Maybe stop newb?
		return Nil, err
	}

handoff:
	if _, err = oldb.processCmd(cmdHandoff{To: newb}); err != nil {
		glog.Errorf("%v cannot handoff to %v: %v", oldb, newb, err)
	}
	return newb, err
}

func (q *qee) isLocalBee(info BeeInfo) bool {
	return q.hive.ID() == info.Hive
}

func (q *qee) defaultLocalBee(id uint64, detached bool) localBee {
	b := localBee{
		qee:      q,
		beeID:    id,
		detached: detached,
		dataCh:   make(chan msgAndHandler, cap(q.dataCh)),
		ctrlCh:   make(chan cmdAndChannel, cap(q.ctrlCh)),
		hive:     q.hive,
		app:      q.app,
		ticker:   time.NewTicker(defaultRaftTick),
	}
	b.setState(q.app.newState())
	return b
}
