package beehive

import "github.com/soheilhy/beehive/Godeps/_workspace/src/github.com/golang/glog"

type detachedBee struct {
	localBee
	h DetachedHandler
}

func (b *detachedBee) start() {
	b.stopped = false
	glog.V(2).Infof("Detached %v started.", b)

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
