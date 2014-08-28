package beehive

type detachedRcvr struct {
	localRcvr
	h DetachedHandler
}

func (r *detachedRcvr) start() {
	go r.h.Start(&r.ctx)
	defer r.h.Stop(&r.ctx)

	for {
		select {
		case d, ok := <-r.dataCh:
			if !ok {
				return
			}
			r.handleMsg(d)

		case c, ok := <-r.ctrlCh:
			if !ok {
				return
			}
			if ok = r.handleCmd(c); !ok {
				return
			}
		}
	}
}

func (r *detachedRcvr) handleMsg(mh msgAndHandler) {
	r.h.Recv(mh.msg, &r.ctx)
}
