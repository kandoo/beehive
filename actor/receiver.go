package actor

type RcvrId struct {
	StageId   StageId
	ActorName ActorName
	Id        uint32
}

func (r *RcvrId) isNil() bool {
	return len(r.StageId) == 0 && len(r.ActorName) == 0 && r.Id == 0
}

func (r *RcvrId) isDetachedId() bool {
	return !r.isNil() && r.Id == detachedRcvrId
}

type receiver interface {
	id() RcvrId
	enque(mh msgAndHandler)
	start()
	handleMsg(mh msgAndHandler)
	handleCmd(cmd routineCmd)
}

type localRcvr struct {
	asyncRoutine
	ctx recvContext
	rId RcvrId
}

func (rcvr *localRcvr) id() RcvrId {
	return rcvr.rId
}

func (rcvr *localRcvr) start() {
	for {
		select {
		case d, ok := <-rcvr.dataCh:
			if !ok {
				return
			}
			rcvr.handleMsg(d)

		case c, ok := <-rcvr.ctrlCh:
			if !ok {
				return
			}
			rcvr.handleCmd(c)
		}
	}
}

func (rcvr *localRcvr) handleMsg(mh msgAndHandler) {
	mh.handler.Recv(mh.msg, &rcvr.ctx)
	rcvr.ctx.stage.collector.collect(mh.msg.From, rcvr.rId, mh.msg)
}

func (rcvr *localRcvr) handleCmd(cmd routineCmd) {
	switch {
	case cmd.cmdType == stopRoutine:
		close(rcvr.dataCh)
		close(rcvr.ctrlCh)
		close(rcvr.waitCh)
	}
}

func (rcvr *localRcvr) enque(mh msgAndHandler) {
	rcvr.dataCh <- mh
}
