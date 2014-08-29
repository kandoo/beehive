package bh

type detachedBee struct {
	localBee
	h DetachedHandler
}

func (r *detachedBee) start() {
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

func (r *detachedBee) handleMsg(mh msgAndHandler) {
	r.h.Rcv(mh.msg, &r.ctx)
}
