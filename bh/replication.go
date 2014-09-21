package bh

import (
	"fmt"
	"math/rand"

	"github.com/golang/glog"
)

type ReplicationStrategy interface {
	// SelectSlaveHives selects nSlaves slave hives that are not in the blackList
	// slice.
	SelectSlaveHives(blackList []HiveID, nSlaves int) []HiveID
}

type BaseReplHandler struct {
	LiveHives map[HiveID]bool
}

func (h *BaseReplHandler) Rcv(msg Msg, ctx RcvContext) error {
	switch d := msg.Data().(type) {
	case HiveJoined:
		h.LiveHives[d.HiveID] = true
	case HiveLeft:
		delete(h.LiveHives, d.HiveID)
	}
	return nil
}

func (h *BaseReplHandler) Map(msg Msg, ctx MapContext) MappedCells {
	return MappedCells{{"D", Key(ctx.Hive().ID())}}
}

func (h *BaseReplHandler) Hives(ctx RcvContext) []HiveID {
	hives := make([]HiveID, 0, len(h.LiveHives))
	for id := range h.LiveHives {
		if id == ctx.Hive().ID() {
			continue
		}
		hives = append(hives, id)
	}
	return hives
}

type ReplicationQuery struct {
	NSlaves   int
	BlackList []HiveID
	Res       chan []HiveID
}

type rndRepliction struct {
	BaseReplHandler
	hive Hive
}

func (h *rndRepliction) Rcv(msg Msg, ctx RcvContext) error {
	switch d := msg.Data().(type) {
	case ReplicationQuery:
		var hives []HiveID
		for _, h := range h.Hives(ctx) {
			found := false
			for _, blk := range d.BlackList {
				if h == blk {
					found = true
					break
				}
			}

			if !found {
				hives = append(hives, h)
			}
		}

		if len(hives) < d.NSlaves {
			d.NSlaves = len(hives)
		}

		rndHives := make([]HiveID, 0, d.NSlaves)
		for _, i := range rand.Perm(d.NSlaves) {
			rndHives = append(rndHives, hives[i])
		}
		d.Res <- rndHives
		return nil
	default:
		return h.BaseReplHandler.Rcv(msg, ctx)
	}
}

func (h *rndRepliction) SelectSlaveHives(blackList []HiveID,
	nSlaves int) []HiveID {

	if nSlaves == 0 {
		return nil
	}

	if blackList == nil {
		blackList = []HiveID{}
	}

	resCh := make(chan []HiveID)
	h.hive.Emit(ReplicationQuery{
		NSlaves:   nSlaves,
		BlackList: blackList,
		Res:       resCh,
	})
	return <-resCh
}

func newRndReplication(h Hive) *rndRepliction {
	r := &rndRepliction{
		BaseReplHandler: BaseReplHandler{
			LiveHives: make(map[HiveID]bool),
		},
		hive: h,
	}
	app := h.NewApp("RndRepl")
	app.Handle(ReplicationQuery{}, r)
	app.Handle(HiveJoined{}, r)
	app.Handle(HiveLeft{}, r)
	app.SetFlags(AppFlagSticky)
	return r
}

func (bee *localBee) replicateTxOnSlave(slave BeeID, txs ...Tx) (int, error) {
	prx := NewProxy(slave.HiveID)
	for i, tx := range txs {
		glog.V(2).Infof("Replicating transaction %v on %v", tx.Seq, slave)
		cmd := NewRemoteCmd(bufferTxCmd{tx}, slave)
		if _, err := prx.SendCmd(&cmd); err != nil {
			return i, err
		}
	}

	return len(txs), nil
}

func (bee *localBee) replicateAllTxOnSlave(slave BeeID) error {
	// FIXME(soheil): Once tx compaction is implemented, we have to replicate the
	// state as well.
	glog.Infof("Replicating the state of %v on a new slave %v", bee.id(), slave)

	n, err := bee.replicateTxOnSlave(slave, bee.txBuf...)
	if err != nil {
		return err
	}

	if n != len(bee.txBuf) {
		return fmt.Errorf("Could only replicate %d transactions", n)
	}

	_, lastTx := bee.lastCommittedTx()
	if lastTx == nil {
		glog.V(2).Infof("No transaction to commit on %v", slave)
		return nil
	}

	return bee.sendCommitToSlave(slave, lastTx.Seq)
}

// replicateTxOnAllSlaves tries to replicate tx on all slaves of this bee. It
// returns the slaves with successful replications and the ones that has failed.
func (bee *localBee) replicateTxOnAllSlaves(tx Tx) ([]BeeID, []BeeID) {
	// TODO(soheil): Add a commit threshold.
	if !bee.isMaster() {
		return nil, nil
	}

	var err error
	allSlaves := bee.slaves()
	deadSlaves := make([]BeeID, 0, len(allSlaves))
	liveSlaves := make([]BeeID, 0, len(allSlaves))
	for _, s := range allSlaves {
		_, err = bee.replicateTxOnSlave(s, tx)
		if err != nil {
			glog.Errorf("Cannot replicate tx %v on bee %v: %v", tx, s, err)
			deadSlaves = append(deadSlaves, s)
			continue
		}

		liveSlaves = append(liveSlaves, s)
	}

	return liveSlaves, deadSlaves
}

func (bee *localBee) sendCommitToAllSlaves(tx TxSeq) error {
	var ret error
	for _, s := range bee.slaves() {
		if err := bee.sendCommitToSlave(s, tx); err != nil {
			ret = err
		}
	}
	return ret
}

func (bee *localBee) sendCommitToSlave(slave BeeID, tx TxSeq) error {
	glog.V(2).Infof("Committing transaction %v on %v", tx, slave)
	cmd := NewRemoteCmd(commitTxCmd{tx}, slave)
	_, err := NewProxy(slave.HiveID).SendCmd(&cmd)
	return err
}
