package bh

type detachedBee struct {
	localBee
	h DetachedHandler
}

func (b *detachedBee) start() {
	go b.h.Start(b)
	defer b.h.Stop(b)

	for !b.stopped {
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
			b.handleCmd(c)
		}
	}
}

func (b *detachedBee) handleMsg(mh msgAndHandler) {
	b.h.Rcv(mh.msg, b)
}
