package actor

import (
	"errors"

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
	keyToRcvrs map[string]receiver
}

func (mapr *mapper) state() State {
	if mapr.ctx.state == nil {
		mapr.ctx.state = newState(string(mapr.ctx.actor.Name()))
	}
	return mapr.ctx.state
}

func (mapr *mapper) detachedRcvrId() RcvrId {
	id := RcvrId{
		StageId:   mapr.ctx.stage.Id(),
		ActorName: mapr.ctx.actor.Name(),
		Id:        detachedRcvrId,
	}
	return id
}

func (mapr *mapper) setDetached(d *detachedRcvr) error {
	if _, ok := mapr.detached(); ok {
		return errors.New("Actor already has a detached handler.")
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
	close(mapr.waitCh)
}

func (mapr *mapper) stopReceivers() {
	stop := routineCmd{stopRoutine, nil, nil}
	if d, ok := mapr.detached(); ok {
		d.ctrlCh <- stop
	}

	for _, v := range mapr.keyToRcvrs {
		switch r := v.(type) {
		case *proxyRcvr:
			r.ctrlCh <- stop
		case *localRcvr:
			r.ctrlCh <- stop
		}
	}
}

func (mapr *mapper) handleCmd(cmd routineCmd) {
	switch {
	case cmd.cmdType == stopRoutine:
		mapr.stopReceivers()
		mapr.closeChannels()

	case cmd.cmdType == findRcvr:
		id := cmd.cmdData.(RcvrId)
		r, ok := mapr.idToRcvrs[id]
		if ok {
			cmd.resCh <- r
			return
		}
		cmd.resCh <- nil
	}
}

func (mapr *mapper) registerDetached(h DetachedHandler) error {
	return mapr.setDetached(mapr.newDetachedRcvr(h))
}

func (mapr *mapper) receiverByKey(dk DictionaryKey) (receiver, bool) {
	r, ok := mapr.keyToRcvrs[dk.String()]
	return r, ok
}

func (mapr *mapper) receiverById(id RcvrId) (receiver, bool) {
	r, ok := mapr.idToRcvrs[id]
	return r, ok
}

func (mapr *mapper) setReceiver(dk DictionaryKey, rcvr receiver) {
	mapr.keyToRcvrs[dk.String()] = rcvr
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
		rcvr, ok := mapr.receiverById(mh.msg.To)
		if !ok {
			if mapr.isLocalRcvr(mh.msg.To) {
				glog.Fatalf("Cannot find a local receiver: %v", mh.msg.To)
			}

			rcvr = mapr.receiver(mh.msg.To)
		}

		if mh.handler == nil && mh.msg.To.Id != detachedRcvrId {
			glog.Fatalf("Handler cannot be nil for receivers.")
		}

		rcvr.enque(mh)
		return
	}

	mapSet := mh.handler.Map(mh.msg, &mapr.ctx)

	rcvr := mapr.anyReceiver(mapSet)
	if rcvr == nil {
		rcvr = mapr.newReceiverForMapSet(mapSet)
	} else {
		mapr.syncReceivers(mapSet, rcvr)
	}

	rcvr.enque(mh)
}

// Locks the map set and returns a new receiver ID if possible, otherwise
// returns the ID of the owner of this map set.
func (mapr *mapper) tryLock(mapSet MapSet) RcvrId {
	mapr.lastRId++
	id := RcvrId{
		ActorName: mapr.ctx.actor.Name(),
		StageId:   mapr.ctx.stage.Id(),
		Id:        mapr.lastRId,
	}

	if mapr.ctx.stage.isIsol() {
		return id
	}

	v := mapr.ctx.stage.registery.storeOrGet(id, mapSet)

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
			waitCh: make(chan interface{}),
		},
		ctx: recvContext{context: mapr.ctx},
		rId: id,
	}
}

func (mapr *mapper) receiver(id RcvrId) receiver {
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
	rcvrId := mapr.tryLock(mapSet)
	rcvr := mapr.receiver(rcvrId)

	for _, dictKey := range mapSet {
		mapr.setReceiver(dictKey, rcvr)
	}

	return rcvr
}
