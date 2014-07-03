package actor

import (
	"fmt"

	"github.com/golang/glog"
)

type mapper struct {
	asyncRoutine
	ctx       context
	receivers map[string]receiver
	lastRId   uint32
}

func (mapr *mapper) state() State {
	if mapr.ctx.state == nil {
		mapr.ctx.state = newState(string(mapr.ctx.actor.Name()))
	}
	return mapr.ctx.state
}

func (mapr *mapper) start() {
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
	// TODO(soheil): Impl this method.
}

func (mapr *mapper) handleCmd(cmd routineCommand) {
	switch cmd {
	case stopActor:
		mapr.stopReceivers()
		mapr.closeChannels()
	}
}

func (mapr *mapper) receiver(dk DictionaryKey) receiver {
	return mapr.receivers[dk.String()]
}

func (mapr *mapper) setReceiver(dk DictionaryKey, rcvr receiver) {
	mapr.receivers[dk.String()] = rcvr
}

func (mapr *mapper) syncReceivers(ms MapSet, rcvr receiver) {
	for _, dictKey := range ms {
		dkRecvr := mapr.receiver(dictKey)
		if dkRecvr == nil {
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
		rcvr := mapr.receiver(dictKey)
		if rcvr != nil {
			return rcvr
		}
	}

	return nil
}

func (mapr *mapper) handleMsg(mh msgAndHandler) {
	mapSet := mh.handler.Map(mh.msg, &mapr.ctx)

	rcvr := mapr.anyReceiver(mapSet)

	if rcvr == nil {
		rcvr = mapr.newReceiver(mapSet)
	}

	mapr.syncReceivers(mapSet, rcvr)

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

func (mapr *mapper) newLocalRcvr(id RcvrId) localRcvr {
	r := localRcvr{
		asyncRoutine: asyncRoutine{
			dataCh: make(chan msgAndHandler, cap(mapr.dataCh)),
			ctrlCh: make(chan routineCommand),
			waitCh: make(chan interface{}),
		},
		rId: id,
	}
	r.ctx = recvContext{
		context: mapr.ctx,
	}
	return r
}

func (mapr *mapper) newReceiver(mapSet MapSet) receiver {
	var rcvr receiver
	rcvrId := mapr.tryLock(mapSet)
	if mapr.isLocalRcvr(rcvrId) {
		fmt.Println("Creating a local receiver")
		r := mapr.newLocalRcvr(rcvrId)
		r.ctx.rcvr = &r
		rcvr = &r
	} else {
		fmt.Println("Creating a proxy receiver")
		r := proxyRcvr{
			localRcvr: mapr.newLocalRcvr(rcvrId),
		}
		r.ctx.rcvr = &r
		rcvr = &r
	}
	go rcvr.start()

	for _, dictKey := range mapSet {
		mapr.setReceiver(dictKey, rcvr)
	}

	return rcvr
}
