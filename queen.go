package beehive

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/soheilhy/beehive/Godeps/_workspace/src/code.google.com/p/go.net/context"
	"github.com/soheilhy/beehive/Godeps/_workspace/src/github.com/golang/glog"
	"github.com/soheilhy/beehive/state"
)

// An applictaion's queen bee is the light weight thread that routes messags
// through the bees of that application.
type qee struct {
	mutex sync.Mutex

	hive *hive
	app  *app

	dataCh  chan msgAndHandler
	ctrlCh  chan cmdAndChannel
	stopped bool

	state State

	idToBees   map[uint64]bee
	cellToBees map[CellKey]bee
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
	b, ok = q.idToBees[id]
	return b, ok
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
		return nil, fmt.Errorf("Bee %v already exists", info.ID)
	}

	b := q.defaultLocalBee(info.ID)
	if _, err := q.hive.node.Process(context.TODO(), addBee(info)); err != nil {
		return nil, err
	}

	if withInitColony {
		b.beeColony = Colony{
			Leader: info.ID,
		}
	}
	q.idToBees[info.ID] = &b
	go b.start()
	return &b, nil
}

func (q *qee) newProxyBee(info BeeInfo) (*proxyBee, error) {
	if q.isLocalBee(info) {
		return nil, errors.New("cannot create proxy for a local bee")
	}

	p, err := q.hive.newProxy(info.Hive)
	if err == nil {
		return nil, err
	}

	b := &proxyBee{
		localBee: q.defaultLocalBee(info.ID),
		proxy:    p,
	}
	// FIXME REFACTOR
	//startHeartbeatBee(id, q.hive)
	q.idToBees[info.ID] = b
	go b.start()
	return b, nil
}

// FIXME REFACTOR
//func (q *qee) detachedBees() []*detachedBee {
//ds := make([]*detachedBee, 0, len(q.idToBees))
//for _, b := range q.idToBees {
//if d, ok := b.(*detachedBee); ok {
//ds = append(ds, b.(*detachedBee))
//}
//}
//return ds
//}

func (q *qee) processCmd(data interface{}) (interface{}, error) {
	ch := make(chan cmdResult)
	q.ctrlCh <- newCmdAndChannel(data, q.app.Name(), 0, ch)
	return (<-ch).get()
}

func (q *qee) sendCmdToBee(bee uint64, data interface{}) (interface{}, error) {
	ch := make(chan cmdResult)
	q.ctrlCh <- newCmdAndChannel(data, q.app.Name(), bee, ch)
	return (<-ch).get()
}

func (q *qee) stopBees() {
	stopCh := make(chan cmdResult)
	stopCmd := newCmdAndChannel(cmdStop{}, q.app.Name(), 0, stopCh)
	for id, b := range q.idToBees {
		glog.V(2).Infof("Stopping bee: %v", id)
		b.enqueCmd(stopCmd)

		_, err := (<-stopCh).get()
		if err != nil {
			glog.Errorf("Error in stopping a bee: %v", err)
		}
	}
}

func (q *qee) handleCmd(cc cmdAndChannel) {
	if cc.cmd.To != 0 {
		if b, ok := q.idToBees[cc.cmd.To]; ok {
			b.enqueCmd(cc)
			return
		}

		// If the bee is a proxy we should try to relay the message.
		if info, err := q.hive.registry.bee(cc.cmd.To); err == nil &&
			!q.isLocalBee(info) {

			if b, err := q.newProxyBee(info); err == nil {
				glog.Warningf("cannot create proxy to %#v", info)
				b.enqueCmd(cc)
				return
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
		glog.V(3).Infof("Stopping bees of %p", q)
		q.stopBees()
		cc.ch <- cmdResult{}
		return

	case cmdFindBee:
		id := cmd.ID
		r, ok := q.idToBees[id]
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
		glog.V(2).Infof("Created a new local bee %v", b)
		return

	case cmdReloadBee:
		_, err := q.reloadBee(cmd.ID)
		cc.ch <- cmdResult{Err: err}
		return

	// FIXME REFACTOR
	//case migrateBeeCmd:
	//q.migrate(cmd.From, cmd.To, cc.ch)
	//return

	//case replaceBeeCmd:
	//q.replaceBee(cmd, cc.ch)
	//return

	//case lockMappedCellsCmd:
	//if cmd.Colony.IsNil() {
	//cc.ch <- cmdResult{Err: errors.New("Colony is nil")}
	//return
	//}

	//id := q.lock(cmd.MappedCells, cmd.Colony, true)
	//if id.IsNil() || !cmd.Colony.IsMaster(id) {
	//cc.ch <- cmdResult{
	//Err: fmt.Errorf("Cannot lock mapset for %#v (locked by %#v)",
	//cmd.Colony, id),
	//}
	//return
	//}
	//bee, err := q.findOrCreateBee(id)
	//if err != nil {
	//cc.ch <- cmdResult{Err: err}
	//return
	//}
	//q.lockLocally(bee, cmd.MappedCells...)
	//cc.ch <- cmdResult{id, nil}

	//case startDetachedCmd:
	//b := q.newDetachedBee(cmd.Handler)
	//q.idToBees[b.ID()] = b
	//go b.start()

	//if cc.ch != nil {
	//cc.ch <- cmdResult{Data: b.id()}
	//}

	default:
		if cc.ch != nil {
			glog.Errorf("Unknown bee command %v", cmd)
			cc.ch <- cmdResult{
				Err: fmt.Errorf("Unknown bee command %v", cmd),
			}
		}
	}
}

func (q *qee) reloadBee(id uint64) (bee, error) {
	b := q.defaultLocalBee(id)
	q.idToBees[id] = &b
	go b.start()
	return &b, nil
}

func (q *qee) invokeMap(mh msgAndHandler) (ms MappedCells) {
	defer func() {
		if r := recover(); r != nil {
			glog.Errorf("Error in map of %s: %v", q.app.Name(), r)
			ms = nil
		}
	}()

	glog.V(2).Infof("Invoking the map function of %v for %v", q, mh.msg)
	return mh.handler.Map(mh.msg, q)
}

func (q *qee) isDetached(id uint64) bool {
	// FIXME REFACTOR CACHE
	b, err := q.hive.registry.bee(id)
	return err == nil && b.Detached
}

func (q *qee) handleMsg(mh msgAndHandler) {
	if mh.msg.IsUnicast() {
		glog.V(2).Infof("Unicast msg: %v", mh.msg)
		bee, ok := q.beeByID(mh.msg.To())
		if !ok {
			info, err := q.hive.registry.bee(mh.msg.To())
			if err != nil {
				glog.Errorf("Cannot find bee %v", mh.msg.To())
			}

			if q.isLocalBee(info) {
				glog.Fatalf("Cannot find a local bee %v", mh.msg.To())
			}

			if bee, ok = q.beeByID(info.ID); !ok {
				glog.Errorf("Cannnot find the remote bee %v", mh.msg.To())
				return
			}
		}

		if mh.handler == nil && !q.isDetached(mh.msg.To()) {
			glog.Fatalf("Handler cannot be nil for bees %v", mh.msg)
		}

		bee.enqueMsg(mh)
		return
	}

	glog.V(2).Infof("Broadcast msg: %v", mh.msg)

	cells := q.invokeMap(mh)
	if cells == nil {
		glog.V(2).Infof("Message dropped: %v", mh.msg)
		return
	}

	if cells.LocalBroadcast() {
		glog.V(2).Infof("%v sends a message to all local bees: %v", q, mh.msg)
		for id, bee := range q.idToBees {
			if _, ok := bee.(*localBee); !ok {
				continue
			}
			if bee.colony().Leader == id {
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
			Colony: lb.colony(),
		}

		if info, err = q.lock(info, cells); err != nil {
			glog.Fatalf("Error in locking the cells: %v", err)
		}

		if info.ID == lb.ID() {
			lb.addMappedCells(cells)
			bee = lb
		} else {
			bee, err = q.beeByCells(cells)
			if err != nil {
				glog.Fatalf("Neither can lock a cell nor can find its bee")
			}
		}
	}
	glog.V(2).Infof("Message sent to bee %v: %v", bee, mh.msg)
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
		glog.Fatalf("Cannot find bee %v: %v", col.Leader, err)
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

	return q.newProxyBee(info)
}

// FIXME REFACTOR
//func (q *qee) beeByKey(dk CellKey) (bee, bool) {
//q.mutex.Lock()
//defer q.mutex.Unlock()

//c, ok := q.cellToBees[dk]
//return c, ok
//}

//func (q *qee) lockLocally(bee bee, dks ...CellKey) {
//q.mutex.Lock()
//defer q.mutex.Unlock()

//glog.V(2).Infof("Bee %v locally locks %v", bee, MappedCells(dks))
//for _, dk := range dks {
//q.cellToBees[dk] = bee
//}
//}

//func (q *qee) syncBees(cells MappedCells, bee bee) {
//colony := bee.colony()
//newCell := false
//for _, dictKey := range cells {
//dkRcvr, ok := q.beeByKey(dictKey)
//if !ok {
//q.lockKey(dictKey, bee, colony)
//newCell = true
//continue
//}

//if dkRcvr == bee {
//continue
//}

//glog.Fatalf("Incosistent shards for keys %v in MappedCells %v", dictKey,
//cells)
//}

//if !newCell {
//return
//}

//if lbee, ok := bee.(*localBee); ok {
//lbee.addMappedCells(cells)
//}
//}

//func (q *qee) nextBeeID(detached bool) uint64 {
//q.lastBeeID++
//return BeeID{
//AppName:  q.app.Name(),
//HiveID:   q.hive.ID(),
//ID:       q.lastBeeID,
//Detached: detached,
//}
//}

//// lock locks the map set for the given bee. If the mapset is already locked,
//// it will return the ID of the owner. If force forced it will force the local
//// bee to lock the map set.
//func (q *qee) lock(mappedCells MappedCells, c Colony, force bool) Colony {
//if q.hive.isolated() {
//return c.Master
//}

//var prevBee BeeID
//reg := &q.hive.registry
//if force {
//// FIXME(soheil): We should migrate the previous owner first.
//reg.syncCall(c.Master, func() {
//prevBee = reg.set(c, mappedCells)
//})
//} else {
//reg.syncCall(c.Master, func() {
//prevBee = reg.storeOrGet(c, mappedCells)
//})
//}

//return prevBee
//}

//func (q *qee) lockKey(dk CellKey, b bee, c Colony) bool {
//q.lockLocally(b, dk)
//if q.hive.isolated() {
//return true
//}

//reg := &q.hive.registry
//reg.syncCall(c.Master, func() {
//reg.storeOrGet(c, []CellKey{dk})
//})
//return true
//}

func (q *qee) isLocalBee(info BeeInfo) bool {
	return q.hive.ID() == info.Hive
}

func (q *qee) defaultLocalBee(id uint64) localBee {
	b := localBee{
		qee:       q,
		beeID:     id,
		beeColony: Colony{Leader: id},
		dataCh:    make(chan msgAndHandler, cap(q.dataCh)),
		ctrlCh:    make(chan cmdAndChannel, cap(q.ctrlCh)),
		hive:      q.hive,
		app:       q.app,
		ticker:    time.NewTicker(defaultRaftTick),
	}
	b.setState(q.app.newState())
	return b
}

// FIXME REFACTOR
//func (q *qee) proxyFromLocal(id uint64, lb *localBee) (*proxyBee, error) {
//if b, ok := q.beeByID(id); ok {
//return nil, fmt.Errorf("Bee already exists: %#v", r)
//}

//p, err := q.hive.newProxy(id.HiveID)
//if err != nil {
//return nil, err
//}
//b := &proxyBee{
//localBee: *lb,
//proxy:    p,
//}
//b.beeID = id
//b.beeColony = Colony{}
//q.idToBees[id] = b
//q.idToBees[lb.ID()] = b
//return b, nil
//}

//func (q *qee) localFromProxy(id uint64, pBee *proxyBee) (*localBee, error) {
//if !q.isLocalBee(id) {
//return nil, fmt.Errorf("Bee ID is a proxy ID: %#v", id)
//}

//if r, ok := q.beeByID(id); ok {
//return nil, fmt.Errorf("Bee already exists: %#v", r)
//}

//b := pBee.localBee
//b.beeID = id
//b.beeColony = Colony{}
//q.idToBees[id] = &b
//q.idToBees[pBee.id()] = &b
//return &b, nil
//}

//func (q *qee) newLocalBee() bee {
//b, err := q.findOrCreateBee(q.nextBeeID(false))
//if err != nil {
//panic("Cannot create local bee")
//}
//return b
//}

//func (q *qee) newDetachedBee(h DetachedHandler) *detachedBee {
//id := q.nextBeeID(true)
//d := &detachedBee{
//localBee: q.defaultLocalBee(id),
//h:        h,
//}
//return d
//}

// FIXME REFACTOR
//func (q *qee) newBeeForMappedCells(cells MappedCells) bee {
//newColony := Colony{
//Master: q.nextBeeID(false),
//}

//var beeID BeeID
//for {
//beeID := q.lock(cells, newColony, false)

//if newColony.Master != beeID {
//q.lastBeeID--
//bee, err := q.findOrCreateBee(beeID)
//if err != nil {
//// we should retry.
//glog.Warningf("Cannot create proxy for %v. Retrying...", beeID)
//continue
//}
//q.lockLocally(bee, cells...)
//return bee
//}
//break
//}

//slaveHives := q.hive.ReplicationStrategy().SelectSlaveHives(
//nil, q.app.ReplicationFactor()-1)
//if len(slaveHives) < q.app.CommitThreshold() {
//glog.Warningf("Could find only %v slaves less than commit threshold of %v",
//len(slaveHives), q.app.CommitThreshold())
//}

//for _, h := range slaveHives {
//slaveID, err := q.hive.createBee(h, q.app.Name())
//if err != nil {
//glog.Fatalf("Failed to create the new bee on %s: %v", h, err)
//}

//glog.V(2).Infof("Adding slave %v to %v", slaveID, newColony.Master)
//newColony.AddSlave(slaveID)
//}

//newColony.Generation++
//beeID = q.lock(cells, newColony, true)

//if newColony.Master != beeID {
//q.lastBeeID--
//bee, err := q.findOrCreateBee(beeID)
//if err != nil {
//panic("FIXME cannot create a bee")
//}

//q.lockLocally(bee, cells...)
//// FIXME(soheil): Stop all the started slaves.
//return bee
//}

//bee, _ := q.findOrCreateBee(beeID)

//q.sendJoinColonyCmd(newColony, beeID)
//for _, s := range newColony.Slaves {
//if err := q.sendJoinColonyCmd(newColony, s); err != nil {
//// FIXME(soheil): We should fix this problem.
//glog.Fatalf("TODO New slave cannot join the colony.")
//}
//}

//q.lockLocally(bee, cells...)
//return bee
//}

//func (q *qee) mappedCellsOfBee(id uint64) MappedCells {
//q.mutex.Lock()
//defer q.mutex.Unlock()

//mc := MappedCells{}
//for k, r := range q.cellToBees {
//if r.id() == id {
//mc = append(mc, k)
//}
//}
//return mc
//}

//func (q *qee) migrate(bee uint64, to uint64, ch chan cmdResult) {
//if beeID.Detached {
//err := fmt.Errorf("Cannot migrate a detached: %#v", beeID)
//resCh <- cmdResult{Err: err}
//return
//}

//prx, err := q.hive.newProxy(to)
//if err != nil {
//resCh <- cmdResult{Err: err}
//return
//}

//glog.V(2).Infof("Migrating %v to %v", beeID, to)
//oldBee, ok := q.beeByID(beeID)
//if !ok {
//err := fmt.Errorf("Bee not found: %v", beeID)
//resCh <- cmdResult{Err: err}
//return
//}

//slaves := oldBee.slaves()

//stopCh := make(chan cmdResult)
//oldBee.enqueCmd(NewLocalCmd(stopCmd{}, BeeID{}, stopCh))
//if _, err = (<-stopCh).get(); err != nil {
//resCh <- cmdResult{Err: err}
//return
//}

//glog.V(2).Infof("Bee %v is stopped for migration", oldBee)

//// TODO(soheil): There is a possibility of a deadlock. If the number of
//// migrrations pass the control channel's buffer size.

//oldColony := Colony{
//Master: oldBee.id(),
//Slaves: slaves,
//}

//newColony := Colony{
//Slaves: slaves,
//}

//for _, s := range slaves {
//if s.HiveID == to {
//newColony.Master = s
//newColony.DelSlave(s)
//break
//}
//}

//if newColony.Master.IsNil() {
//newColony.Master = BeeID{HiveID: to, AppName: beeID.AppName}

//cmd := NewRemoteCmd(createBeeCmd{}, newColony.Master)
//data, err := prx.sendCmd(&cmd)
//if err != nil {
//glog.Errorf("Error in creating a new bee: %s", err)
//resCh <- cmdResult{nil, err}
//return
//}

//newColony.Master = data.(BeeID)
//} else {
//newSlave := q.newLocalBee()
//newSlave.setState(oldBee.txState())
//newSlave.setTxBuffer(oldBee.txBuffer())
//newColony.AddSlave(newSlave.id())
//}

//glog.V(2).Infof("Created a new bee for migration: %v", newColony.Master)

//newBee, err := q.proxyFromLocal(newColony.Master, oldBee.(*localBee))
//if err != nil {
//resCh <- cmdResult{nil, err}
//return
//}
//mappedCells := q.mappedCellsOfBee(oldBee.id())
//q.lockLocally(newBee, mappedCells...)

//go newBee.start()

//glog.V(2).Infof("Local bee is converted to proxy %v", newBee)

//cmd := RemoteCmd{
//CmdTo: newColony.Master.queen(),
//Cmd: replaceBeeCmd{
//OldBees:     oldColony,
//NewBees:     newColony,
//State:       oldBee.txState().(*inMemoryState),
//TxBuf:       oldBee.txBuffer(),
//MappedCells: mappedCells,
//},
//}
//_, err = prx.sendCmd(&cmd)
//if err != nil {
//glog.Errorf("Error in replacing the bee: %s", err)
//return
//}

//for _, s := range newColony.Slaves {
//q.sendJoinColonyCmd(newColony, s)
//}

//resCh <- cmdResult{newBee, nil}
//}

//func (q *qee) replaceBee(cmd replaceBeeCmd, resCh chan cmdResult) {
//if !q.isLocalBee(cmd.NewBees.Master) {
//err := fmt.Errorf("Cannot replace with a non-local bee: %v",
//cmd.NewBees.Master)
//resCh <- cmdResult{Err: err}
//return
//}

//b, ok := q.beeByID(cmd.NewBees.Master)
//if !ok {
//err := fmt.Errorf("Cannot find bee: %v", cmd.NewBees.Master)
//resCh <- cmdResult{Err: err}
//return
//}

//q.sendJoinColonyCmd(cmd.NewBees, b.id())
//q.idToBees[cmd.OldBees.Master] = b

//glog.V(2).Infof("Replicating the state of %v on %v", cmd.OldBees.Master,
//cmd.NewBees.Master)

//newState := b.txState()
//for name, oldDict := range cmd.State.Dicts {
//newDict := newState.Dict(DictName(name))
//oldDict.ForEach(func(k Key, v Value) {
//newDict.Put(k, v)
//})
//}

//b.setTxBuffer(cmd.TxBuf)

//reg := &q.hive.registry
//reg.syncCall(cmd.NewBees.Master, func() {
//reg.set(cmd.NewBees, cmd.MappedCells)
//})

//glog.V(2).Infof("Bee %v locked %v", cmd.MappedCells, cmd.NewBees)
//q.lockLocally(b, cmd.MappedCells...)

//resCh <- cmdResult{b.id(), nil}
//}

//func (q *qee) sendJoinColonyCmd(c Colony, to uint64) error {
//if q.hive.ID() == to.HiveID {
//jch := make(chan cmdResult)
//b := q.idToBees[to]
//b.enqueCmd(NewLocalCmd(joinColonyCmd{c}, b.ID(), jch))
//_, err := (<-jch).get()
//return err
//}

//cmd := NewRemoteCmd(joinColonyCmd{c}, to)
//p, err := q.hive.newProxy(to.HiveID)
//if err != nil {
//return err
//}
//_, err = p.sendCmd(&cmd)
//return err
//}
