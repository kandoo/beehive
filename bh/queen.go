package bh

import (
	"errors"
	"fmt"
	"sync"

	"github.com/golang/glog"
)

type QeeID struct {
	Hive HiveID
	App  AppName
}

// An applictaion's queen bee is the light weight thread that routes messags
// through the bees of that application.
type qee struct {
	mutex sync.Mutex
	hive  *hive
	app   *app

	dataCh chan msgAndHandler
	ctrlCh chan LocalCmd

	state TxState

	lastBeeID  uint64
	idToBees   map[BeeID]bee
	cellToBees map[CellKey]bee
}

func (q *qee) setDetached(d *detachedBee) error {
	q.idToBees[d.id()] = d
	return nil
}

func (q *qee) detachedBees() []*detachedBee {
	ds := make([]*detachedBee, 0, len(q.idToBees))
	for id, b := range q.idToBees {
		if !id.Detached {
			continue
		}
		ds = append(ds, b.(*detachedBee))
	}
	return ds
}

func (q *qee) start() {
	for {
		select {
		case d, ok := <-q.dataCh:
			if !ok {
				return
			}
			q.handleMsg(d)

		case cmd, ok := <-q.ctrlCh:
			if !ok {
				return
			}
			q.handleCmd(cmd)
		}
	}
}

func (q *qee) stopBees() {
	stopCh := make(chan CmdResult)
	stopCmd := NewLocalCmd(stopCmd{}, BeeID{}, stopCh)
	for id, b := range q.idToBees {
		glog.V(2).Infof("Stopping bee: %v", id)
		b.enqueCmd(stopCmd)

		_, err := (<-stopCh).get()
		if err != nil {
			glog.Errorf("Error in stopping a bee: %v", err)
		}
	}
}

func (q *qee) handleCmd(lcmd LocalCmd) {
	if lcmd.CmdTo.ID != 0 {
		if bee, ok := q.idToBees[lcmd.CmdTo]; ok {
			cmdResCh := lcmd.ResCh
			if cmdResCh != nil {
				lcmd.ResCh = make(chan CmdResult)
			}
			bee.enqueCmd(lcmd)
			res := <-lcmd.ResCh
			if cmdResCh != nil {
				cmdResCh <- res
			}
			return
		}

		if lcmd.ResCh != nil {
			lcmd.ResCh <- CmdResult{
				Err: fmt.Errorf("Cannot find bee for the command %#v", lcmd),
			}
		}
		return
	}

	switch cmd := lcmd.Cmd.(type) {
	case stopCmd:
		glog.V(3).Infof("Stopping bees of %p", q)
		q.stopBees()
		lcmd.ResCh <- CmdResult{}
		return

	case findBeeCmd:
		id := cmd.BeeID
		r, ok := q.idToBees[id]
		if ok {
			lcmd.ResCh <- CmdResult{Data: r}
			return
		}

		err := fmt.Errorf("No bee found: %#v", id)
		lcmd.ResCh <- CmdResult{Err: err}
		return

	case createBeeCmd:
		r := q.newLocalBee()
		lcmd.ResCh <- CmdResult{Data: r.id()}
		glog.V(2).Infof("Created a new local bee: %#v", r.id())
		return

	case migrateBeeCmd:
		q.migrate(cmd.From, cmd.To, lcmd.ResCh)
		return

	case replaceBeeCmd:
		q.replaceBee(cmd, lcmd.ResCh)
		return

	case lockMappedCellsCmd:
		if cmd.Colony.IsNil() {
			lcmd.ResCh <- CmdResult{Err: errors.New("Colony is nil")}
			return
		}

		id := q.lock(cmd.MappedCells, cmd.Colony, true)
		if id.IsNil() || !cmd.Colony.IsMaster(id) {
			lcmd.ResCh <- CmdResult{
				Err: fmt.Errorf("Cannot lock mapset for %#v (locked by %#v)",
					cmd.Colony, id),
			}
			return
		}

		q.lockLocally(q.findOrCreateBee(id), cmd.MappedCells...)
		lcmd.ResCh <- CmdResult{id, nil}

	case startDetachedCmd:
		b := q.newDetachedBee(cmd.Handler)
		q.idToBees[b.id()] = b
		go b.start()

		if lcmd.ResCh != nil {
			lcmd.ResCh <- CmdResult{Data: b.id()}
		}

	default:
		if lcmd.ResCh != nil {
			glog.Errorf("Unknown bee command %v", cmd)
			lcmd.ResCh <- CmdResult{
				Err: fmt.Errorf("Unknown bee command %v", cmd),
			}
		}
	}
}

func (q *qee) beeByKey(dk CellKey) (bee, bool) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	r, ok := q.cellToBees[dk]
	return r, ok
}

func (q *qee) beeByID(id BeeID) (bee, bool) {
	r, ok := q.idToBees[id]
	return r, ok
}

func (q *qee) lockLocally(bee bee, dks ...CellKey) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	glog.V(2).Infof("Bee %v locally locks %v", bee.id(), MappedCells(dks))
	for _, dk := range dks {
		q.cellToBees[dk] = bee
	}
}

func (q *qee) syncBees(cells MappedCells, bee bee) {
	colony := BeeColony{
		Master: bee.id(),
		Slaves: bee.slaves(),
	}

	newCell := false
	for _, dictKey := range cells {
		dkRcvr, ok := q.beeByKey(dictKey)
		if !ok {
			q.lockKey(dictKey, bee, colony)
			newCell = true
			continue
		}

		if dkRcvr == bee {
			continue
		}

		glog.Fatalf("Incosistent shards for keys %v in MappedCells %v", dictKey,
			cells)
	}

	if !newCell {
		return
	}

	if lbee, ok := bee.(*localBee); ok {
		lbee.addMappedCells(cells)
	}
}

func (q *qee) anyBee(ms MappedCells) bee {
	for _, dictKey := range ms {
		bee, ok := q.beeByKey(dictKey)
		if ok {
			return bee
		}
	}

	return nil
}

func (q *qee) invokeMap(mh msgAndHandler) (ms MappedCells) {
	defer func() {
		if r := recover(); r != nil {
			glog.Errorf("Error in map of %s: %v", q.app.Name(), r)
			ms = nil
		}
	}()

	return mh.handler.Map(mh.msg, q)
}

func (q *qee) handleMsg(mh msgAndHandler) {
	if mh.msg.isUnicast() {
		glog.V(2).Infof("Unicast msg: %v", mh.msg)
		bee, ok := q.beeByID(mh.msg.To())
		if !ok {
			if q.isLocalBee(mh.msg.To()) {
				glog.Fatalf("Cannot find a local bee %v", mh.msg.To())
			}

			bee = q.findOrCreateBee(mh.msg.To())
		}

		if mh.handler == nil && !mh.msg.To().Detached {
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
		glog.V(2).Infof("Sending a message to all local bees: %v", mh.msg)
		for _, bee := range q.idToBees {
			if _, ok := bee.(*localBee); !ok {
				return
			}

			bee.enqueMsg(mh)
		}
		return
	}

	bee := q.anyBee(cells)
	if bee == nil {
		bee = q.newBeeForMappedCells(cells)
		if lbee, ok := bee.(*localBee); ok {
			lbee.addMappedCells(cells)
		}
	} else {
		q.syncBees(cells, bee)
	}

	glog.V(2).Infof("Message sent to bee %v: %v", bee.id(), mh.msg)
	bee.enqueMsg(mh)
}

func (q *qee) nextBeeID(detached bool) BeeID {
	q.lastBeeID++
	return BeeID{
		AppName:  q.app.Name(),
		HiveID:   q.hive.ID(),
		ID:       q.lastBeeID,
		Detached: detached,
	}
}

// lock locks the map set for the given bee. If the mapset is already locked,
// it will return the ID of the owner. If force forced it will force the local
// bee to lock the map set.
func (q *qee) lock(mappedCells MappedCells, c BeeColony, force bool) BeeID {
	if q.hive.isolated() {
		return c.Master
	}

	var prevBee BeeID
	reg := &q.hive.registry
	if force {
		// FIXME(soheil): We should migrate the previous owner first.
		reg.syncCall(c.Master, func() {
			prevBee = reg.set(c, mappedCells)
		})
	} else {
		reg.syncCall(c.Master, func() {
			prevBee = reg.storeOrGet(c, mappedCells)
		})
	}

	return prevBee
}

func (q *qee) lockKey(dk CellKey, b bee, c BeeColony) bool {
	q.lockLocally(b, dk)
	if q.hive.isolated() {
		return true
	}

	reg := &q.hive.registry
	reg.syncCall(c.Master, func() {
		reg.storeOrGet(c, []CellKey{dk})
	})
	return true
}

func (q *qee) isLocalBee(id BeeID) bool {
	return q.hive.ID() == id.HiveID
}

func (q *qee) defaultLocalBee(id BeeID) localBee {
	return localBee{
		qee:    q,
		beeID:  id,
		dataCh: make(chan msgAndHandler, cap(q.dataCh)),
		ctrlCh: make(chan LocalCmd, cap(q.ctrlCh)),
		hive:   q.hive,
		app:    q.app,
		state:  nil,
	}
}

func (q *qee) proxyFromLocal(id BeeID, lBee *localBee) (*proxyBee,
	error) {

	if q.isLocalBee(id) {
		return nil, fmt.Errorf("Bee ID is a local ID: %#v", id)
	}

	if r, ok := q.beeByID(id); ok {
		return nil, fmt.Errorf("Bee already exists: %#v", r)
	}

	b := &proxyBee{
		localBee: *lBee,
		proxy:    NewProxy(id.HiveID),
	}
	b.beeID = id
	b.beeColony = BeeColony{}
	q.idToBees[id] = b
	q.idToBees[lBee.id()] = b
	return b, nil
}

func (q *qee) localFromProxy(id BeeID, pBee *proxyBee) (*localBee,
	error) {

	if !q.isLocalBee(id) {
		return nil, fmt.Errorf("Bee ID is a proxy ID: %#v", id)
	}

	if r, ok := q.beeByID(id); ok {
		return nil, fmt.Errorf("Bee already exists: %#v", r)
	}

	b := pBee.localBee
	b.beeID = id
	b.beeColony = BeeColony{}
	q.idToBees[id] = &b
	q.idToBees[pBee.id()] = &b
	return &b, nil
}

func (q *qee) newLocalBee() bee {
	return q.findOrCreateBee(q.nextBeeID(false))
}

func (q *qee) findOrCreateBee(id BeeID) bee {
	if r, ok := q.beeByID(id); ok {
		return r
	}

	l := q.defaultLocalBee(id)

	var bee bee
	if q.isLocalBee(id) {
		bee = &l
	} else {
		bee = &proxyBee{
			localBee: l,
			proxy:    NewProxy(id.HiveID),
		}
		startHeartbeatBee(id, q.hive)
	}

	q.idToBees[id] = bee
	go bee.start()

	return bee
}

func (q *qee) newDetachedBee(h DetachedHandler) *detachedBee {
	id := q.nextBeeID(true)
	d := &detachedBee{
		localBee: q.defaultLocalBee(id),
		h:        h,
	}
	return d
}

func (q *qee) newBeeForMappedCells(cells MappedCells) bee {
	newColony := BeeColony{
		Master: q.nextBeeID(false),
	}

	beeID := q.lock(cells, newColony, false)

	if newColony.Master != beeID {
		q.lastBeeID--
		bee := q.findOrCreateBee(beeID)
		q.lockLocally(bee, cells...)
		return bee
	}

	slaveHives := q.hive.ReplicationStrategy().SelectSlaveHives(
		nil, q.app.ReplicationFactor()-1)
	if len(slaveHives) < q.app.CommitThreshold() {
		glog.Warningf("Could find only %v slaves less than commit threshold of %v",
			len(slaveHives), q.app.CommitThreshold())
	}

	for _, h := range slaveHives {
		slaveID, err := CreateBee(h, q.app.Name())
		if err != nil {
			glog.Fatalf("Failed to create the new bee on %s: %v", h, err)
		}

		glog.V(2).Infof("Adding slave %v to %v", slaveID, newColony.Master)
		newColony.AddSlave(slaveID)
	}

	newColony.Generation++
	beeID = q.lock(cells, newColony, true)

	if newColony.Master != beeID {
		q.lastBeeID--
		bee := q.findOrCreateBee(beeID)
		q.lockLocally(bee, cells...)
		// FIXME(soheil): Stop all the started slaves.
		return bee
	}

	bee := q.findOrCreateBee(beeID)

	q.sendJoinColonyCmd(newColony, beeID)
	for _, s := range newColony.Slaves {
		if err := q.sendJoinColonyCmd(newColony, s); err != nil {
			// FIXME(soheil): We should fix this problem.
			glog.Fatalf("New slave cannot join the colony.")
		}
	}

	q.lockLocally(bee, cells...)
	return bee
}

func (q *qee) mappedCellsOfBee(id BeeID) MappedCells {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	mc := MappedCells{}
	for k, r := range q.cellToBees {
		if r.id() == id {
			mc = append(mc, k)
		}
	}
	return mc
}

func (q *qee) migrate(beeID BeeID, to HiveID, resCh chan CmdResult) {
	if beeID.Detached {
		err := fmt.Errorf("Cannot migrate a detached: %#v", beeID)
		resCh <- CmdResult{Err: err}
		return
	}

	glog.V(2).Infof("Migrating %v to %v", beeID, to)
	oldBee, ok := q.beeByID(beeID)
	if !ok {
		err := fmt.Errorf("Bee not found: %v", beeID)
		resCh <- CmdResult{Err: err}
		return
	}

	slaves := oldBee.slaves()

	stopCh := make(chan CmdResult)
	oldBee.enqueCmd(NewLocalCmd(stopCmd{}, BeeID{}, stopCh))
	_, err := (<-stopCh).get()
	if err != nil {
		resCh <- CmdResult{Err: err}
		return
	}

	glog.V(2).Infof("Bee %v is stopped for migration", oldBee.id())

	// TODO(soheil): There is a possibility of a deadlock. If the number of
	// migrrations pass the control channel's buffer size.

	oldColony := BeeColony{
		Master: oldBee.id(),
		Slaves: slaves,
	}

	newColony := BeeColony{
		Slaves: slaves,
	}

	for _, s := range slaves {
		if s.HiveID == to {
			newColony.Master = s
			newColony.DelSlave(s)
			break
		}
	}

	prx := NewProxy(to)

	if newColony.Master.IsNil() {
		newColony.Master = BeeID{HiveID: to, AppName: beeID.AppName}

		cmd := NewRemoteCmd(createBeeCmd{}, newColony.Master)
		data, err := prx.SendCmd(&cmd)
		if err != nil {
			glog.Errorf("Error in creating a new bee: %s", err)
			resCh <- CmdResult{nil, err}
			return
		}

		newColony.Master = data.(BeeID)
	} else {
		newSlave := q.newLocalBee()
		newSlave.setState(oldBee.txState())
		newSlave.setTxBuffer(oldBee.txBuffer())
		newColony.AddSlave(newSlave.id())
	}

	glog.V(2).Infof("Created a new bee for migration: %v", newColony.Master)

	newBee, err := q.proxyFromLocal(newColony.Master, oldBee.(*localBee))
	if err != nil {
		resCh <- CmdResult{nil, err}
		return
	}
	mappedCells := q.mappedCellsOfBee(oldBee.id())
	q.lockLocally(newBee, mappedCells...)

	go newBee.start()

	glog.V(2).Infof("Local bee is converted to proxy %v", newBee.id())

	cmd := RemoteCmd{
		CmdTo: newColony.Master.queen(),
		Cmd: replaceBeeCmd{
			OldBees:     oldColony,
			NewBees:     newColony,
			State:       oldBee.txState().(*inMemoryState),
			TxBuf:       oldBee.txBuffer(),
			MappedCells: mappedCells,
		},
	}
	_, err = prx.SendCmd(&cmd)
	if err != nil {
		glog.Errorf("Error in replacing the bee: %s", err)
		return
	}

	for _, s := range newColony.Slaves {
		q.sendJoinColonyCmd(newColony, s)
	}

	resCh <- CmdResult{newBee, nil}
}

func (q *qee) replaceBee(cmd replaceBeeCmd, resCh chan CmdResult) {
	if !q.isLocalBee(cmd.NewBees.Master) {
		err := fmt.Errorf("Cannot replace with a non-local bee: %v",
			cmd.NewBees.Master)
		resCh <- CmdResult{Err: err}
		return
	}

	b, ok := q.beeByID(cmd.NewBees.Master)
	if !ok {
		err := fmt.Errorf("Cannot find bee: %v", cmd.NewBees.Master)
		resCh <- CmdResult{Err: err}
		return
	}

	q.sendJoinColonyCmd(cmd.NewBees, b.id())
	q.idToBees[cmd.OldBees.Master] = b

	glog.V(2).Infof("Replicating the state of %v on %v", cmd.OldBees.Master,
		cmd.NewBees.Master)

	newState := b.txState()
	for name, oldDict := range cmd.State.Dicts {
		newDict := newState.Dict(DictName(name))
		oldDict.ForEach(func(k Key, v Value) {
			newDict.Put(k, v)
		})
	}

	b.setTxBuffer(cmd.TxBuf)

	reg := &q.hive.registry
	reg.syncCall(cmd.NewBees.Master, func() {
		reg.set(cmd.NewBees, cmd.MappedCells)
	})

	glog.V(2).Infof("Bee %v locked %v", cmd.MappedCells, cmd.NewBees)
	q.lockLocally(b, cmd.MappedCells...)

	resCh <- CmdResult{b.id(), nil}
}

func (q *qee) sendJoinColonyCmd(c BeeColony, to BeeID) error {
	if q.hive.ID() == to.HiveID {
		jch := make(chan CmdResult)
		b := q.idToBees[to]
		b.enqueCmd(NewLocalCmd(joinColonyCmd{c}, b.id(), jch))
		_, err := (<-jch).get()
		return err
	}

	cmd := NewRemoteCmd(joinColonyCmd{c}, to)
	_, err := NewProxy(to.HiveID).SendCmd(&cmd)
	return err
}
