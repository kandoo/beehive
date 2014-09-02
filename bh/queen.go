package bh

import (
	"encoding/gob"
	"errors"
	"fmt"

	"github.com/golang/glog"
)

const (
	detachedBeeId = 0
)

// An applictaion's queen bee is the light weight thread that routes messags
// through the bees of that application.
type qee struct {
	asyncRoutine
	ctx       mapContext
	lastRId   uint32
	idToBees  map[BeeId]bee
	keyToBees map[DictionaryKey]bee
}

func (q *qee) state() State {
	if q.ctx.state == nil {
		q.ctx.state = newState(q.ctx.app)
	}
	return q.ctx.state
}

func (q *qee) detachedBeeId() BeeId {
	id := BeeId{
		HiveId:  q.ctx.hive.Id(),
		AppName: q.ctx.app.Name(),
		Id:      detachedBeeId,
	}
	return id
}

func (q *qee) setDetached(d *detachedBee) error {
	if _, ok := q.detached(); ok {
		return errors.New("App already has a detached handler.")
	}

	q.idToBees[q.detachedBeeId()] = d
	return nil
}

func (q *qee) detached() (*detachedBee, bool) {
	d, ok := q.idToBees[q.detachedBeeId()]
	if !ok {
		return nil, false
	}
	return d.(*detachedBee), ok
}

func (q *qee) start() {
	if d, ok := q.detached(); ok {
		go d.start()
	}

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

func (q *qee) closeChannels() {
	close(q.dataCh)
	close(q.ctrlCh)
}

func (q *qee) stopBees() {
	stopCh := make(chan asyncResult)
	stopCmd := routineCmd{stopCmd, nil, stopCh}
	if d, ok := q.detached(); ok {
		d.ctrlCh <- stopCmd
		_, err := (<-stopCh).get()
		if err != nil {
			glog.Errorf("Error in stopping the detached thread: %v", err)
		}
	}

	bees := make(map[bee]bool)
	for _, v := range q.keyToBees {
		bees[v] = true
	}

	for k, _ := range bees {
		switch r := k.(type) {
		case *proxyBee:
			r.ctrlCh <- stopCmd
		case *localBee:
			r.ctrlCh <- stopCmd
		}
		_, err := (<-stopCh).get()
		if err != nil {
			glog.Errorf("Error in stopping a bee: %v", err)
		}
	}
}

func (q *qee) handleCmd(cmd routineCmd) {
	switch cmd.cmdType {
	case stopCmd:
		glog.V(3).Infof("Stopping bees of %p", q)
		q.stopBees()
		q.closeChannels()
		cmd.resCh <- asyncResult{}

	case findBeeCmd:
		id := cmd.cmdData.(BeeId)
		r, ok := q.idToBees[id]
		if ok {
			cmd.resCh <- asyncResult{r, nil}
			return
		}

		err := errors.New(fmt.Sprintf("No bee found: %+v", id))
		cmd.resCh <- asyncResult{nil, err}

	case createBeeCmd:
		r := q.newLocalBee()
		glog.V(2).Infof("Created a new local bee: %+v", r.id())
		cmd.resCh <- asyncResult{r.id(), nil}

	case migrateBeeCmd:
		m := cmd.cmdData.(migrateBeeCmdData)
		q.migrate(m.From, m.To, cmd.resCh)

	case replaceBeeCmd:
		d := cmd.cmdData.(replaceBeeCmdData)
		q.replaceBee(d, cmd.resCh)
	}
}

func (q *qee) registerDetached(h DetachedHandler) error {
	return q.setDetached(q.newDetachedBee(h))
}

func (q *qee) beeByKey(dk DictionaryKey) (bee, bool) {
	r, ok := q.keyToBees[dk]
	return r, ok
}

func (q *qee) beeById(id BeeId) (bee, bool) {
	r, ok := q.idToBees[id]
	return r, ok
}

func (q *qee) setBee(dk DictionaryKey, bee bee) {
	q.keyToBees[dk] = bee
}

func (q *qee) syncBees(ms MapSet, bee bee) {
	for _, dictKey := range ms {
		dkRcvr, ok := q.beeByKey(dictKey)
		if !ok {
			q.lockKey(dictKey, bee)
			continue
		}

		if dkRcvr == bee {
			continue
		}

		glog.Fatalf("Incosistent shards for keys %v in MapSet %v", dictKey,
			ms)
	}
}

func (q *qee) anyBee(ms MapSet) bee {
	for _, dictKey := range ms {
		bee, ok := q.beeByKey(dictKey)
		if ok {
			return bee
		}
	}

	return nil
}

func (q *qee) callMap(mh msgAndHandler) (ms MapSet) {
	defer func() {
		if r := recover(); r != nil {
			glog.Errorf("Error in map of %s: %v", q.ctx.app.Name(), r)
			ms = nil
		}
	}()

	return mh.handler.Map(mh.msg, &q.ctx)
}

func (q *qee) handleMsg(mh msgAndHandler) {
	if mh.msg.isUnicast() {
		glog.V(2).Infof("Unicast msg: %+v", mh.msg)
		bee, ok := q.beeById(mh.msg.To())
		if !ok {
			if q.isLocalBee(mh.msg.To()) {
				glog.Fatalf("Cannot find a local bee: %v", mh.msg.To)
			}

			bee = q.findOrCreateBee(mh.msg.To())
		}

		if mh.handler == nil && mh.msg.To().Id != detachedBeeId {
			glog.Fatalf("Handler cannot be nil for bees: %+v, %+v", mh, mh.msg)
		}

		bee.enqueMsg(mh)
		return
	}

	glog.V(2).Infof("Broadcast msg: %+v", mh.msg)

	mapSet := q.callMap(mh)
	if mapSet == nil {
		glog.V(2).Infof("Message dropped: %+v", mh)
		return
	}

	if mapSet.LocalBroadcast() {
		glog.V(2).Infof("Sending a message to all local bees: %v", mh.msg)
		for _, bee := range q.idToBees {
			bee.enqueMsg(mh)
		}
		return
	}

	bee := q.anyBee(mapSet)
	if bee == nil {
		bee = q.newBeeForMapSet(mapSet)
	} else {
		q.syncBees(mapSet, bee)
	}

	glog.V(2).Infof("Sending to bee: %v", bee.id())
	bee.enqueMsg(mh)
}

func (q *qee) nextBeeId() BeeId {
	q.lastRId++
	return BeeId{
		AppName: q.ctx.app.Name(),
		HiveId:  q.ctx.hive.Id(),
		Id:      q.lastRId,
	}
}

// Locks the map set and returns a new bee ID if possible, otherwise
// returns the ID of the owner of this map set.
func (q *qee) lock(mapSet MapSet, force bool) BeeId {
	id := q.nextBeeId()
	if q.ctx.hive.isolated() {
		return id
	}

	var v beeRegVal
	if force {
		v = q.ctx.hive.registery.set(id, mapSet)
	} else {
		v = q.ctx.hive.registery.storeOrGet(id, mapSet)
	}

	if v.HiveId == id.HiveId && v.BeeId == id.Id {
		return id
	}

	q.lastRId--
	id.HiveId = v.HiveId
	id.Id = v.BeeId
	return id
}

func (q *qee) lockKey(dk DictionaryKey, bee bee) bool {
	q.setBee(dk, bee)
	if q.ctx.hive.isolated() {
		return true
	}

	q.ctx.hive.registery.storeOrGet(bee.id(), []DictionaryKey{dk})

	return true
}

func (q *qee) isLocalBee(id BeeId) bool {
	return q.ctx.hive.Id() == id.HiveId
}

func (q *qee) defaultLocalBee(id BeeId) localBee {
	return localBee{
		asyncRoutine: asyncRoutine{
			dataCh: make(chan msgAndHandler, cap(q.dataCh)),
			ctrlCh: make(chan routineCmd),
		},
		ctx: q.ctx.newRcvContext(),
		rId: id,
	}
}

func (q *qee) proxyFromLocal(id BeeId, lBee *localBee) (*proxyBee,
	error) {

	if q.isLocalBee(id) {
		return nil, errors.New(fmt.Sprintf("Bee ID is a local ID: %+v", id))
	}

	if r, ok := q.beeById(id); ok {
		return nil, errors.New(fmt.Sprintf("Bee already exists: %+v", r))
	}

	r := &proxyBee{
		localBee: *lBee,
	}
	r.rId = id
	r.ctx.bee = r
	q.idToBees[id] = r
	q.idToBees[lBee.id()] = r
	return r, nil
}

func (q *qee) localFromProxy(id BeeId, pBee *proxyBee) (*localBee,
	error) {

	if !q.isLocalBee(id) {
		return nil, errors.New(fmt.Sprintf("Bee ID is a proxy ID: %+v", id))
	}

	if r, ok := q.beeById(id); ok {
		return nil, errors.New(fmt.Sprintf("Bee already exists: %+v", r))
	}

	r := pBee.localBee
	r.rId = id
	r.ctx.bee = &r
	q.idToBees[id] = &r
	q.idToBees[pBee.id()] = &r
	return &r, nil
}

func (q *qee) newLocalBee() bee {
	return q.findOrCreateBee(q.nextBeeId())
}

func (q *qee) findOrCreateBee(id BeeId) bee {
	if r, ok := q.beeById(id); ok {
		return r
	}

	l := q.defaultLocalBee(id)

	var bee bee
	if q.isLocalBee(id) {
		b := &l
		b.ctx.bee = b
		bee = b
	} else {
		b := &proxyBee{
			localBee: l,
		}
		b.ctx.bee = b
		bee = b

		startHeartbeatBee(id, q.ctx.hive)
	}

	q.idToBees[id] = bee
	go bee.start()

	return bee
}

func (q *qee) newDetachedBee(h DetachedHandler) *detachedBee {
	d := &detachedBee{
		localBee: q.defaultLocalBee(q.detachedBeeId()),
		h:        h,
	}
	d.ctx.bee = d
	return d
}

func (q *qee) newBeeForMapSet(mapSet MapSet) bee {
	beeId := q.lock(mapSet, false)
	bee := q.findOrCreateBee(beeId)

	for _, dictKey := range mapSet {
		q.setBee(dictKey, bee)
	}

	return bee
}

func (q *qee) mapSetOfBee(id BeeId) MapSet {
	ms := MapSet{}
	for k, r := range q.keyToBees {
		if r.id() == id {
			ms = append(ms, k)
		}
	}
	return ms
}

func (q *qee) migrate(beeId BeeId, to HiveId, resCh chan asyncResult) {
	if beeId.isDetachedId() {
		err := errors.New(fmt.Sprintf("Cannot migrate a detached: %+v", beeId))
		resCh <- asyncResult{nil, err}
		return
	}

	oldBee, ok := q.beeById(beeId)
	if !ok {
		err := errors.New(fmt.Sprintf("Bee not found: %+v", oldBee))
		resCh <- asyncResult{nil, err}
		return
	}

	stopCh := make(chan asyncResult)
	oldBee.enqueCmd(routineCmd{stopCmd, nil, stopCh})
	_, err := (<-stopCh).get()
	if err != nil {
		resCh <- asyncResult{nil, err}
		return
	}

	glog.V(2).Infof("Received stopped: %+v", oldBee)

	// TODO(soheil): There is a possibility of a deadlock. If the number of
	// migrrations pass the control channel's buffer size.
	conn, err := dialHive(to)
	if err != nil {
		resCh <- asyncResult{nil, err}
		return
	}

	defer conn.Close()

	enc := gob.NewEncoder(conn)
	dec := gob.NewDecoder(conn)

	if err := enc.Encode(hiveHandshake{ctrlHandshake}); err != nil {
		glog.Errorf("Cannot encode handshake: %+v", err)
		resCh <- asyncResult{nil, err}
		return
	}

	id := BeeId{HiveId: to, AppName: beeId.AppName}
	if err := enc.Encode(hiveRemoteCommand{createBeeCmd, id}); err != nil {
		glog.Errorf("Cannot encode command: %+v", err)
		resCh <- asyncResult{nil, err}
		return
	}

	if err := dec.Decode(&id); err != nil {
		glog.V(2).Infof("Cannot decode the new bee: %+v", err)
		resCh <- asyncResult{nil, err}
		return
	}

	glog.V(2).Infof("Got the new bee: %+v", id)

	newBee, err := q.proxyFromLocal(id, oldBee.(*localBee))
	if err != nil {
		resCh <- asyncResult{nil, err}
		return
	}

	glog.V(2).Infof("Created a proxy for the new bee: %+v", newBee)

	if err := enc.Encode(hiveRemoteCommand{replaceBeeCmd, id}); err != nil {
		glog.Errorf("Cannot encode replace command: %v", err)
		return
	}

	mapSet := q.mapSetOfBee(oldBee.id())
	replaceData := replaceBeeCmdData{
		OldBee: oldBee.id(),
		NewBee: newBee.id(),
		State:  oldBee.state().(*inMemoryState),
		MapSet: mapSet,
	}
	if err := enc.Encode(replaceData); err != nil {
		glog.Errorf("Cannot encode replace command data: %v", err)
		return
	}

	newId := BeeId{}
	if err := dec.Decode(&newId); err != nil {
		glog.Errorf("Cannot replace the bee: %v", err)
		return
	}

	for _, dictKey := range mapSet {
		q.setBee(dictKey, newBee)
	}

	go newBee.start()
	resCh <- asyncResult{newBee, nil}
}

func (q *qee) replaceBee(d replaceBeeCmdData, resCh chan asyncResult) {
	if !q.isLocalBee(d.NewBee) {
		err := errors.New(
			fmt.Sprintf("Cannot replace with a non-local bee: %+v", d.NewBee))
		resCh <- asyncResult{nil, err}
		return
	}

	r, ok := q.beeById(d.NewBee)
	if !ok {
		err := errors.New(fmt.Sprintf("Cannot find bee: %+v", d.NewBee))
		resCh <- asyncResult{nil, err}
		return
	}

	newState := r.state()
	for name, oldDict := range d.State.Dicts {
		newDict := newState.Dict(DictionaryName(name))
		oldDict.ForEach(func(k Key, v Value) {
			newDict.Put(k, v)
		})
	}
	glog.V(2).Infof("Replicated the state of %+v on %+v", d.OldBee, d.NewBee)

	q.ctx.hive.registery.set(d.NewBee, d.MapSet)
	glog.V(2).Infof("Locked the mapset %+v for %+v", d.MapSet, d.NewBee)

	for _, dictKey := range d.MapSet {
		q.setBee(dictKey, r)
	}

	resCh <- asyncResult{r, nil}
}
