package bh

import (
	"encoding/gob"
	"errors"
	"fmt"

	"github.com/golang/glog"
)

const (
	detachedRcvrId = 0
)

type mapper struct {
	asyncRoutine
	ctx        context
	lastRId    uint32
	idToRcvrs  map[RcvrId]receiver
	keyToRcvrs map[DictionaryKey]receiver
}

func (mapr *mapper) state() State {
	if mapr.ctx.state == nil {
		mapr.ctx.state = newState(string(mapr.ctx.app.Name()))
	}
	return mapr.ctx.state
}

func (mapr *mapper) detachedRcvrId() RcvrId {
	id := RcvrId{
		StageId: mapr.ctx.stage.Id(),
		AppName: mapr.ctx.app.Name(),
		Id:      detachedRcvrId,
	}
	return id
}

func (mapr *mapper) setDetached(d *detachedRcvr) error {
	if _, ok := mapr.detached(); ok {
		return errors.New("App already has a detached handler.")
	}

	mapr.idToRcvrs[mapr.detachedRcvrId()] = d
	return nil
}

func (mapr *mapper) detached() (*detachedRcvr, bool) {
	d, ok := mapr.idToRcvrs[mapr.detachedRcvrId()]
	if !ok {
		return nil, false
	}
	return d.(*detachedRcvr), ok
}

func (mapr *mapper) start() {
	if d, ok := mapr.detached(); ok {
		go d.start()
	}

	for {
		select {
		case d, ok := <-mapr.dataCh:
			if !ok {
				return
			}
			mapr.handleMsg(d)

		case cmd, ok := <-mapr.ctrlCh:
			if !ok {
				return
			}
			mapr.handleCmd(cmd)
		}
	}
}

func (mapr *mapper) closeChannels() {
	close(mapr.dataCh)
	close(mapr.ctrlCh)
}

func (mapr *mapper) stopReceivers() {
	stopCh := make(chan asyncResult)
	stopCmd := routineCmd{stopCmd, nil, stopCh}
	if d, ok := mapr.detached(); ok {
		d.ctrlCh <- stopCmd
		_, err := (<-stopCh).get()
		if err != nil {
			glog.Errorf("Error in stopping the detached thread: %v", err)
		}
	}

	rcvrs := make(map[receiver]bool)
	for _, v := range mapr.keyToRcvrs {
		rcvrs[v] = true
	}

	for k, _ := range rcvrs {
		switch r := k.(type) {
		case *proxyRcvr:
			r.ctrlCh <- stopCmd
		case *localRcvr:
			r.ctrlCh <- stopCmd
		}
		_, err := (<-stopCh).get()
		if err != nil {
			glog.Errorf("Error in stopping a rcvr: %v", err)
		}
	}
}

func (mapr *mapper) handleCmd(cmd routineCmd) {
	switch cmd.cmdType {
	case stopCmd:
		glog.V(3).Infof("Stopping receivers of %p", mapr)
		mapr.stopReceivers()
		mapr.closeChannels()
		cmd.resCh <- asyncResult{}

	case findRcvrCmd:
		id := cmd.cmdData.(RcvrId)
		r, ok := mapr.idToRcvrs[id]
		if ok {
			cmd.resCh <- asyncResult{r, nil}
			return
		}

		err := errors.New(fmt.Sprintf("No receiver found: %+v", id))
		cmd.resCh <- asyncResult{nil, err}

	case createRcvrCmd:
		r := mapr.newLocalReceiver()
		glog.V(2).Infof("Created a new local receiver: %+v", r.id())
		cmd.resCh <- asyncResult{r.id(), nil}

	case migrateRcvrCmd:
		m := cmd.cmdData.(migrateRcvrCmdData)
		mapr.migrate(m.From, m.To, cmd.resCh)

	case replaceRcvrCmd:
		d := cmd.cmdData.(replaceRcvrCmdData)
		mapr.replaceRcvr(d, cmd.resCh)
	}
}

func (mapr *mapper) registerDetached(h DetachedHandler) error {
	return mapr.setDetached(mapr.newDetachedRcvr(h))
}

func (mapr *mapper) receiverByKey(dk DictionaryKey) (receiver, bool) {
	r, ok := mapr.keyToRcvrs[dk]
	return r, ok
}

func (mapr *mapper) receiverById(id RcvrId) (receiver, bool) {
	r, ok := mapr.idToRcvrs[id]
	return r, ok
}

func (mapr *mapper) setReceiver(dk DictionaryKey, rcvr receiver) {
	mapr.keyToRcvrs[dk] = rcvr
}

func (mapr *mapper) syncReceivers(ms MapSet, rcvr receiver) {
	for _, dictKey := range ms {
		dkRecvr, ok := mapr.receiverByKey(dictKey)
		if !ok {
			mapr.lockKey(dictKey, rcvr)
			continue
		}

		if dkRecvr == rcvr {
			continue
		}

		glog.Fatalf("Incosistent shards for keys %v in MapSet %v", dictKey,
			ms)
	}
}

func (mapr *mapper) anyReceiver(ms MapSet) receiver {
	for _, dictKey := range ms {
		rcvr, ok := mapr.receiverByKey(dictKey)
		if ok {
			return rcvr
		}
	}

	return nil
}

func (mapr *mapper) handleMsg(mh msgAndHandler) {
	if mh.msg.isUnicast() {
		glog.V(2).Infof("Unicast msg: %+v", mh.msg)
		rcvr, ok := mapr.receiverById(mh.msg.To())
		if !ok {
			if mapr.isLocalRcvr(mh.msg.To()) {
				glog.Fatalf("Cannot find a local receiver: %v", mh.msg.To)
			}

			rcvr = mapr.findOrCreateReceiver(mh.msg.To())
		}

		if mh.handler == nil && mh.msg.To().Id != detachedRcvrId {
			glog.Fatalf("Handler cannot be nil for receivers: %+v, %+v", mh, mh.msg)
		}

		rcvr.enqueMsg(mh)
		return
	}

	glog.V(2).Infof("Broadcast msg: %+v", mh.msg)

	mapSet := mh.handler.Map(mh.msg, &mapr.ctx)

	rcvr := mapr.anyReceiver(mapSet)
	if rcvr == nil {
		rcvr = mapr.newReceiverForMapSet(mapSet)
	} else {
		mapr.syncReceivers(mapSet, rcvr)
	}

	glog.V(2).Infof("Sending to receiver: %v", rcvr.id())
	rcvr.enqueMsg(mh)
}

func (mapr *mapper) nextRcvrId() RcvrId {
	mapr.lastRId++
	return RcvrId{
		AppName: mapr.ctx.app.Name(),
		StageId: mapr.ctx.stage.Id(),
		Id:      mapr.lastRId,
	}
}

// Locks the map set and returns a new receiver ID if possible, otherwise
// returns the ID of the owner of this map set.
func (mapr *mapper) lock(mapSet MapSet, force bool) RcvrId {
	id := mapr.nextRcvrId()
	if mapr.ctx.stage.isIsol() {
		return id
	}

	var v regVal
	if force {
		v = mapr.ctx.stage.registery.set(id, mapSet)
	} else {
		v = mapr.ctx.stage.registery.storeOrGet(id, mapSet)
	}

	if v.StageId == id.StageId && v.RcvrId == id.Id {
		return id
	}

	mapr.lastRId--
	id.StageId = v.StageId
	id.Id = v.RcvrId
	return id
}

func (mapr *mapper) lockKey(dk DictionaryKey, rcvr receiver) bool {
	mapr.setReceiver(dk, rcvr)
	if mapr.ctx.stage.isIsol() {
		return true
	}

	mapr.ctx.stage.registery.storeOrGet(rcvr.id(), []DictionaryKey{dk})

	return true
}

func (mapr *mapper) isLocalRcvr(id RcvrId) bool {
	return mapr.ctx.stage.Id() == id.StageId
}

func (mapr *mapper) defaultLocalRcvr(id RcvrId) localRcvr {
	return localRcvr{
		asyncRoutine: asyncRoutine{
			dataCh: make(chan msgAndHandler, cap(mapr.dataCh)),
			ctrlCh: make(chan routineCmd),
		},
		ctx: recvContext{context: mapr.ctx},
		rId: id,
	}
}

func (mapr *mapper) proxyFromLocal(id RcvrId, lRcvr *localRcvr) (*proxyRcvr,
	error) {

	if mapr.isLocalRcvr(id) {
		return nil, errors.New(fmt.Sprintf("Receiver ID is a local ID: %+v", id))
	}

	if r, ok := mapr.receiverById(id); ok {
		return nil, errors.New(fmt.Sprintf("Rcvr already exists: %+v", r))
	}

	r := &proxyRcvr{
		localRcvr: *lRcvr,
	}
	r.rId = id
	r.ctx.rcvr = r
	mapr.idToRcvrs[id] = r
	mapr.idToRcvrs[lRcvr.id()] = r
	return r, nil
}

func (mapr *mapper) localFromProxy(id RcvrId, pRcvr *proxyRcvr) (*localRcvr,
	error) {

	if !mapr.isLocalRcvr(id) {
		return nil, errors.New(fmt.Sprintf("Receiver ID is a proxy ID: %+v", id))
	}

	if r, ok := mapr.receiverById(id); ok {
		return nil, errors.New(fmt.Sprintf("Rcvr already exists: %+v", r))
	}

	r := pRcvr.localRcvr
	r.rId = id
	r.ctx.rcvr = &r
	mapr.idToRcvrs[id] = &r
	mapr.idToRcvrs[pRcvr.id()] = &r
	return &r, nil
}

func (mapr *mapper) newLocalReceiver() receiver {
	return mapr.findOrCreateReceiver(mapr.nextRcvrId())
}

func (mapr *mapper) findOrCreateReceiver(id RcvrId) receiver {
	if r, ok := mapr.receiverById(id); ok {
		return r
	}

	l := mapr.defaultLocalRcvr(id)

	var rcvr receiver
	if mapr.isLocalRcvr(id) {
		r := &l
		r.ctx.rcvr = r
		rcvr = r
	} else {
		r := &proxyRcvr{
			localRcvr: l,
		}
		r.ctx.rcvr = r
		rcvr = r
	}

	mapr.idToRcvrs[id] = rcvr
	go rcvr.start()

	return rcvr
}

func (mapr *mapper) newDetachedRcvr(h DetachedHandler) *detachedRcvr {
	d := &detachedRcvr{
		localRcvr: mapr.defaultLocalRcvr(mapr.detachedRcvrId()),
		h:         h,
	}
	d.ctx.rcvr = d
	return d
}

func (mapr *mapper) newReceiverForMapSet(mapSet MapSet) receiver {
	rcvrId := mapr.lock(mapSet, false)
	rcvr := mapr.findOrCreateReceiver(rcvrId)

	for _, dictKey := range mapSet {
		mapr.setReceiver(dictKey, rcvr)
	}

	return rcvr
}

func (mapr *mapper) mapSetOfRcvr(id RcvrId) MapSet {
	ms := MapSet{}
	for k, r := range mapr.keyToRcvrs {
		if r.id() == id {
			ms = append(ms, k)
		}
	}
	return ms
}

func (mapr *mapper) migrate(rcvrId RcvrId, to StageId, resCh chan asyncResult) {
	if rcvrId.isDetachedId() {
		err := errors.New(fmt.Sprintf("Cannot migrate detached: %+v", rcvrId))
		resCh <- asyncResult{nil, err}
		return
	}

	oldRcvr, ok := mapr.receiverById(rcvrId)
	if !ok {
		err := errors.New(fmt.Sprintf("Receiver not found: %+v", oldRcvr))
		resCh <- asyncResult{nil, err}
		return
	}

	stopCh := make(chan asyncResult)
	oldRcvr.enqueCmd(routineCmd{stopCmd, nil, stopCh})
	_, err := (<-stopCh).get()
	if err != nil {
		resCh <- asyncResult{nil, err}
		return
	}

	glog.V(2).Infof("Received stopped: %+v", oldRcvr)

	// TODO(soheil): There is a possibility of a deadlock. If the number of
	// migrrations pass the control channel's buffer size.
	conn, err := dialStage(to)
	if err != nil {
		resCh <- asyncResult{nil, err}
		return
	}

	defer conn.Close()

	enc := gob.NewEncoder(conn)
	dec := gob.NewDecoder(conn)

	if err := enc.Encode(stageHandshake{ctrlHandshake}); err != nil {
		glog.Errorf("Cannot encode handshake: %+v", err)
		resCh <- asyncResult{nil, err}
		return
	}

	id := RcvrId{StageId: to, AppName: rcvrId.AppName}
	if err := enc.Encode(stageRemoteCommand{createRcvrCmd, id}); err != nil {
		glog.Errorf("Cannot encode command: %+v", err)
		resCh <- asyncResult{nil, err}
		return
	}

	if err := dec.Decode(&id); err != nil {
		glog.V(2).Infof("Cannot decode the new receiver: %+v", err)
		resCh <- asyncResult{nil, err}
		return
	}

	glog.V(2).Infof("Got the new receiver: %+v", id)

	newRcvr, err := mapr.proxyFromLocal(id, oldRcvr.(*localRcvr))
	if err != nil {
		resCh <- asyncResult{nil, err}
		return
	}

	glog.V(2).Infof("Created a proxy for the new receiver: %+v", newRcvr)

	if err := enc.Encode(stageRemoteCommand{replaceRcvrCmd, id}); err != nil {
		glog.Errorf("Cannot encode replace command: %v", err)
		return
	}

	mapSet := mapr.mapSetOfRcvr(oldRcvr.id())
	replaceData := replaceRcvrCmdData{
		OldRcvr: oldRcvr.id(),
		NewRcvr: newRcvr.id(),
		State:   oldRcvr.state().(*inMemoryState),
		MapSet:  mapSet,
	}
	if err := enc.Encode(replaceData); err != nil {
		glog.Errorf("Cannot encode replace command data: %v", err)
		return
	}

	for _, dictKey := range mapSet {
		mapr.setReceiver(dictKey, newRcvr)
	}

	go newRcvr.start()
	resCh <- asyncResult{newRcvr, nil}
}

func (mapr *mapper) replaceRcvr(d replaceRcvrCmdData, resCh chan asyncResult) {
	if !mapr.isLocalRcvr(d.NewRcvr) {
		err := errors.New(
			fmt.Sprintf("Cannot replace with a non-local receiver: %+v", d.NewRcvr))
		resCh <- asyncResult{nil, err}
		return
	}

	r, ok := mapr.receiverById(d.NewRcvr)
	if !ok {
		err := errors.New(fmt.Sprintf("Cannot find receiver: %+v", d.NewRcvr))
		resCh <- asyncResult{nil, err}
		return
	}

	newState := r.state()
	for name, oldDict := range d.State.Dicts {
		newDict := newState.Dict(DictionaryName(name))
		for k, v := range oldDict.All() {
			newDict.Set(k, v)
		}
	}
	glog.V(2).Infof("Replicated the state of %+v on %+v", d.OldRcvr, d.NewRcvr)

	mapr.ctx.stage.registery.set(d.NewRcvr, d.MapSet)
	glog.V(2).Infof("Locked the mapset %+v for %+v", d.MapSet, d.NewRcvr)

	for _, dictKey := range d.MapSet {
		mapr.setReceiver(dictKey, r)
	}

	resCh <- asyncResult{r, nil}
}
