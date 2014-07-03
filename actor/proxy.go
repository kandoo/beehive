package actor

import (
	"encoding/gob"
	"net"
	"time"

	"github.com/golang/glog"
)

const (
	proxyProto                 = "tcp"
	minWaitInMs  time.Duration = 100
	maxWaitInMs                = 1000
	waitStepInMs               = 10
)

type proxyRcvr struct {
	localRcvr
	conn    net.Conn
	encoder *gob.Encoder
	decoder *gob.Decoder
}

func (r *proxyRcvr) handleMsg(mh msgAndHandler) {
	err := r.encoder.Encode(mh.msg)
	if err != nil {
		glog.Errorf("Cannot encode message: %v", err)
	}
}

func (r *proxyRcvr) dial() {
	// FIXME(soheil): This can't scale. We can only support 65k remote receivers.
	// There should be one connection per remote stage and with that we can
	// support 65k remote controllers.
	step := time.Duration(waitStepInMs)
	waitMs := minWaitInMs
	for {
		c, err := net.Dial(proxyProto, string(r.rId.StageId))
		if err == nil {
			r.conn = c
			break
		}

		// TODO(soheil): What if the pair has just crashed? We need to return error
		// and return from start and try to regrab the locks again. Then panic if
		// neither can be successful.
		glog.Errorf("Cannot connect to %s: %v", r.rId.StageId, err)
		time.Sleep(waitMs * time.Millisecond)

		if waitMs > maxWaitInMs {
			waitMs = maxWaitInMs
			continue
		}

		if waitMs < maxWaitInMs {
			waitMs += step
			step *= 2
		}
	}

	r.encoder = gob.NewEncoder(r.conn)
	r.decoder = gob.NewDecoder(r.conn)

	err := r.encoder.Encode(&r.rId)
	if err != nil {
		glog.Fatalf("Cannot handshake with peer: %v", err)
	}

	peerOk := false
	r.decoder.Decode(&peerOk)
	if !peerOk {
		glog.Fatalf("Peer cannot find receiver: %+v", r.rId)
	}
}

func (r *proxyRcvr) hangup() {
	if r.conn != nil {
		r.conn.Close()
	}
}

// TODO(soheil): Maybe start should return an error.
func (r *proxyRcvr) start() {
	r.dial()
	defer r.hangup()

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
			r.handleCmd(c)
		}
	}
}
