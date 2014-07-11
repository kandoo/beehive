package actor

import (
	"encoding/gob"
	"net"

	"github.com/golang/glog"
)

type stageHandshake struct {
	Type   routineCmdType
	RcvrId RcvrId
}

func (s *stage) handleConn(conn net.Conn) {
	defer conn.Close()

	dec := gob.NewDecoder(conn)
	enc := gob.NewEncoder(conn)

	var hs stageHandshake
	err := dec.Decode(&hs)
	if err != nil {
		glog.Errorf("Cannot decode handshake.")
		return
	}

	a, ok := s.actor(hs.RcvrId.ActorName)
	if !ok {
		glog.Errorf("Cannot find actor: %s", hs.RcvrId.ActorName)
		return
	}

	resCh := make(chan asyncResult)
	a.mapper.ctrlCh <- routineCmd{hs.Type, hs.RcvrId, resCh}
	res, err := (<-resCh).get()
	if err != nil {
		glog.Error(err)
		return
	}

	rcvr := res.(receiver)
	id := rcvr.id()
	enc.Encode(id)

	if hs.Type == newRcvrCmd {
		return
	}

	for {
		m := msg{}
		if err := dec.Decode(&m); err != nil {
			glog.Errorf("Cannot decode message: %v", err)
			return
		}

		a.mapper.dataCh <- msgAndHandler{&m, a.handlers[m.Type()]}
	}
}

func (s *stage) listen() {
	l, err := net.Listen("tcp", s.config.StageAddr)
	if err != nil {
		glog.Fatal("Cannot start listener: %v", err)
	}
	defer l.Close()

	glog.V(1).Infof("Network server listening at: %s", s.config.StageAddr)
	for {
		c, err := l.Accept()
		if err != nil {
			glog.Errorf("Error in accept %s", err)
			continue
		}
		glog.V(2).Infof("Accepting a new connection: %v -> %v", c.RemoteAddr(),
			c.LocalAddr())
		go s.handleConn(c)
	}
}
