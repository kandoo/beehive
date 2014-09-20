package bh

import (
	"errors"
	"time"

	"github.com/golang/glog"
)

type failureHandler struct {
	lockTimeout time.Duration
}

func (h *failureHandler) Rcv(msg Msg, ctx RcvContext) error {
	ctx.AbortTx()

	bFailed := msg.Data().(beeFailed)
	b, ok := ctx.(*localBee)
	if !ok {
		return errors.New("Cannot handle failures of a detached or a proxy.")
	}

	if err := b.hive.registry.tryLockApp(b.id()); err != nil {
		ctx.Snooze(h.lockTimeout)
	}

	defer func() {
		if err := b.hive.registry.unlockApp(b.id()); err != nil {
			glog.Fatalf("Cannot unlock the application: %v", err)
		}
	}()

	bCol := b.colony()
	switch {
	case bCol.IsMaster(bFailed.id):
		b.handleMasterFailure(bFailed.id)

	case bCol.IsSlave(bFailed.id) && b.isMaster():
		b.handleSlaveFailure(bFailed.id)
	}

	return nil
}

func (h *failureHandler) Map(msg Msg, ctx MapContext) MappedCells {
	return MappedCells{}
}

func (bee *localBee) handleSlaveFailure(slaveID BeeID) {
	oldCol := bee.colony()
	newCol := oldCol.DeepCopy()
	if !newCol.DelSlave(slaveID) {
		return
	}

	glog.Warningf("Bee %v has a failed slave %v", bee.id(), slaveID)

	newCol.Generation++

	var err error
	var cmd RemoteCmd
	newSlaveID := BeeID{}

	slaves := make([]HiveID, 0, len(oldCol.Slaves))
	for _, s := range oldCol.Slaves {
		slaves = append(slaves, s.HiveID)
	}
	newSlaveHive := bee.hive.ReplicationStrategy().SelectSlaveHives(slaves, 1)
	if len(newSlaveHive) == 0 {
		glog.Errorf("Cannot find a slave hive to replace %v", slaveID)
		goto register
	}

	newSlaveID, err = CreateBee(newSlaveHive[0], bee.app.Name())
	if err != nil {
		glog.Errorf("Cannot find a slave hive to replace %v", slaveID)
		goto register
	}

	newCol.AddSlave(newSlaveID)
	cmd = NewRemoteCmd(joinColonyCmd{newCol}, newSlaveID)
	if _, err = NewProxy(newSlaveID.HiveID).SendCmd(&cmd); err != nil {
		newCol.DelSlave(newSlaveID)
		newSlaveID = BeeID{}
		goto register
	}

	glog.V(2).Infof("Created slave %v for %v", newSlaveID, newCol.Master)

register:
	cells := bee.mappedCells()
	glog.V(2).Infof("Trying to replace %v with %v in the registry for %v", oldCol,
		newCol, cells)
	oldCol, err = bee.hive.registry.compareAndSet(oldCol, newCol, cells)
	if err != nil {
		glog.Errorf("Bee %v has an expired colony %v", bee.id(), newCol)
		bee.stop()
		return
	}

	bee.setColony(newCol)

	if newSlaveID.IsNil() {
		return
	}

	// FIXME(soheil): We are ignoring replication error.
	bee.replicateAllTxOnSlave(newSlaveID)
}

func (bee *localBee) handleMasterFailure(masterID BeeID) {
	oldCol := bee.colony()
	newCol := oldCol.DeepCopy()
	if !newCol.IsMaster(masterID) {
		return
	}

	if !newCol.DelSlave(bee.beeID) {
		return
	}

	glog.Warningf("Bee %v has a failed master", bee.id(), masterID)

	failedSlaves := make([]BeeID, 0, len(newCol.Slaves))
	slaveTxInfo := make(map[BeeID]TxInfo)
	for _, s := range newCol.Slaves {
		cmd := NewRemoteCmd(getTxInfoCmd{}, s)
		d, err := NewProxy(s.HiveID).SendCmd(&cmd)
		if err != nil {
			failedSlaves = append(failedSlaves, s)
			continue
		}

		info := d.(TxInfo)
		slaveTxInfo[s] = info
	}

	for s, info := range slaveTxInfo {
		if info.Generation > bee.gen() {
			glog.Errorf("Slave %v has an expired generation", s)
			bee.stop()
			return
		}
	}

	newCol.Master = bee.beeID
	newCol.Generation++

	maxInfo := TxInfo{
		Generation:    bee.gen(),
		LastCommitted: bee.lastCommittedTx().Seq,
		LastBuffered:  bee.txBuf[len(bee.txBuf)-1].Seq,
	}

	lastBufferedSlave := bee.id()

	for s, info := range slaveTxInfo {
		if info.Generation < maxInfo.Generation {
			continue
		}

		if info.LastCommitted > maxInfo.LastCommitted {
			maxInfo.LastCommitted = info.LastCommitted
		}

		if info.LastBuffered > maxInfo.LastBuffered {
			maxInfo.LastBuffered = info.LastBuffered
			lastBufferedSlave = s
		}
	}

	if maxInfo.LastCommitted > maxInfo.LastBuffered {
		glog.Errorf("Inconsistencies in slave state")
		// TODO(soheil): Maybe it's not a good thing to ignore such inconsistencies?
		// Should we stop the inconsistent bees?
		maxInfo.LastCommitted = maxInfo.LastBuffered
	}

	if lastBufferedSlave != bee.id() {
		cmd := RemoteCmd{
			Cmd: getTx{
				From: bee.txBuf[len(bee.txBuf)-1].Seq + 1,
				To:   maxInfo.LastBuffered,
			},
			CmdTo: lastBufferedSlave,
		}
		data, err := NewProxy(lastBufferedSlave.HiveID).SendCmd(&cmd)
		if err != nil {
			glog.Fatal("This part has not bee implemented yet.")
		}

		for _, tx := range data.([]Tx) {
			if tx.Seq <= maxInfo.LastCommitted {
				tx.Status = TxCommitted
			}
			bee.txBuf = append(bee.txBuf, tx)
		}
	}

	for s, info := range slaveTxInfo {
		if info.LastBuffered == maxInfo.LastBuffered {
			continue
		}

		var i int
		for i = len(bee.txBuf) - 1; i >= 0; i-- {
			if bee.txBuf[i].Seq == maxInfo.LastBuffered {
				break
			}
		}

		for ; i < len(bee.txBuf); i++ {
			cmd := RemoteCmd{
				Cmd: bufferTxCmd{
					Tx: bee.txBuf[i],
				},
				CmdTo: s,
			}
			_, err := NewProxy(s.HiveID).SendCmd(&cmd)
			if err != nil {
				glog.Fatal("This part has not bee implemented yet.")
			}
		}
	}

	for s, info := range slaveTxInfo {
		if info.LastCommitted == maxInfo.LastBuffered {
			continue
		}

		cmd := RemoteCmd{
			Cmd: commitTxCmd{
				Seq: maxInfo.LastCommitted,
			},
			CmdTo: s,
		}
		_, err := NewProxy(s.HiveID).SendCmd(&cmd)
		if err != nil {
			glog.Fatal("This part has not bee implemented yet.")
		}
	}

	// FIXME(soheil): Handle failed bees.

	oldCol, err := bee.hive.registry.compareAndSet(oldCol, newCol,
		bee.mappedCells())
	if err != nil {
		glog.Errorf("Bee %#v has a expired colony %#v", bee.id(), newCol)
		bee.stop()
		return
	}

	bee.setColony(newCol)

	for s, _ := range slaveTxInfo {
		cmd := RemoteCmd{
			Cmd: joinColonyCmd{
				Colony: newCol,
			},
			CmdTo: s,
		}
		_, err := NewProxy(s.HiveID).SendCmd(&cmd)
		if err != nil {
			glog.Fatal("This part has not bee implemented yet.")
		}
	}
}
