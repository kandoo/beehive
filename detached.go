package beehive

import "github.com/kandoo/beehive/Godeps/_workspace/src/github.com/golang/glog"

type detachedBee struct {
	localBee
	h DetachedHandler
}

func (b *detachedBee) start() {
	b.status = beeStatusStarted
	go b.h.Start(b)
	defer b.h.Stop(b)

	glog.V(2).Infof("%v started", b)
	for b.status == beeStatusStarted {
		select {
		case d := <-b.dataCh:
			b.handleMsg(d)

		case c := <-b.ctrlCh:
			b.handleCmd(c)
		}
	}
}

func (b *detachedBee) String() string {
	return "detached " + b.localBee.String()
}

func (b *detachedBee) handleMsg(mh msgAndHandler) {
	b.h.Rcv(mh.msg, b)
}
