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
	start()

	enqueMsg(mh msgAndHandler)
	enqueCmd(cmd routineCmd)

	handleMsg(mh msgAndHandler)
	// Handles a command and returns false if the receiver should stop.
	handleCmd(cmd routineCmd) bool
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
			if ok = rcvr.handleCmd(c); !ok {
				return
			}
		}
	}
}

func (rcvr *localRcvr) handleMsg(mh msgAndHandler) {
	mh.handler.Recv(mh.msg, &rcvr.ctx)
	rcvr.ctx.stage.collector.collect(mh.msg.From(), rcvr.rId, mh.msg)
}

func (rcvr *localRcvr) handleCmd(cmd routineCmd) bool {
	switch cmd.cmdType {
	case stopCmd:
		cmd.resCh <- asyncResult{}
		return false
	}

	return true
}

func (rcvr *localRcvr) enqueMsg(mh msgAndHandler) {
	rcvr.dataCh <- mh
}

func (rcvr *localRcvr) enqueCmd(cmd routineCmd) {
	rcvr.ctrlCh <- cmd
}
