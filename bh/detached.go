package bh

type detachedBee struct {
	localBee
	h DetachedHandler
}

func (b *detachedBee) start() {
	go b.h.Start(&b.ctx)
	defer b.h.Stop(&b.ctx)

	for {
		select {
		case d, ok := <-b.dataCh:
			if !ok {
				return
			}
			b.handleMsg(d)

		case c, ok := <-b.ctrlCh:
			if !ok {
				return
			}
			if ok = b.handleCmd(c); !ok {
				return
			}
		}
	}
}

func (b *detachedBee) handleMsg(mh msgAndHandler) {
	b.h.Rcv(mh.msg, &b.ctx)
}
