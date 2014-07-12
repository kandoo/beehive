package actor

import (
	"encoding/gob"
	"net"

	"github.com/golang/glog"
)

type handShakeType int

const (
	ctrlHandshake handShakeType = iota
	dataHandshake
)

type stageHandshake struct {
	Type handShakeType
}

type stageRemoteCommand struct {
	Type routineCmdType
	RcvrId
}

func (s *stage) handleCtrlConn(conn net.Conn, dec *gob.Decoder,
	enc *gob.Encoder) {

	for {
		var cmd stageRemoteCommand
		if err := dec.Decode(&cmd); err != nil {
			glog.Errorf("Cannot decode the command: %v", err)
			return
		}

		if cmd.StageId != s.id {
			glog.Errorf("Command is not for this stage: %+v", cmd)
			return
		}

		switch cmd.Type {
		case newRcvrCmd:
			a, ok := s.actor(cmd.ActorName)
			if !ok {
				glog.Errorf("Cannot find actor: %v", cmd.RcvrId.ActorName)
				return
			}

			resCh := make(chan asyncResult)
			a.mapper.ctrlCh <- routineCmd{newRcvrCmd, nil, resCh}
			res, err := (<-resCh).get()
			if err != nil {
				glog.Error(err)
				return
			}

			if err := enc.Encode(res.(RcvrId)); err != nil {
				glog.Errorf("Cannot encode receiver id: %v", err)
				return
			}
		}
	}
}

func (s *stage) handleDataConn(conn net.Conn, dec *gob.Decoder,
	enc *gob.Encoder) {

	var to RcvrId
	if err := dec.Decode(&to); err != nil {
		glog.Errorf("Cannot decode the target receiver id for data conn: %+v", err)
		return
	}

	a, ok := s.actor(to.ActorName)
	if !ok {
		glog.Errorf("Cannot find actor: %s", to.ActorName)
		return
	}

	resCh := make(chan asyncResult)
	a.mapper.ctrlCh <- routineCmd{findRcvrCmd, to, resCh}
	res, err := (<-resCh).get()
	if err != nil {
		glog.Error(err)
		return
	}

	rcvr := res.(receiver)
	id := rcvr.id()
	enc.Encode(id)
	glog.V(2).Infof("Encoded receiver id: %+v", id)

	for {
		m := msg{}
		if err := dec.Decode(&m); err != nil {
			glog.Errorf("Cannot decode message: %v", err)
			return
		}

		glog.V(3).Infof("Received a message from peer: %+v", m)
		a.mapper.dataCh <- msgAndHandler{&m, a.handlers[m.Type()]}
	}
}

func (s *stage) handleConn(conn net.Conn) {
	defer conn.Close()

	dec := gob.NewDecoder(conn)
	enc := gob.NewEncoder(conn)

	var hs stageHandshake
	err := dec.Decode(&hs)
	if err != nil {
		glog.Errorf("Cannot decode server handshake: %+v", err)
		return
	}

	glog.V(2).Infof("Peer handshaked: %+v", hs)

	switch hs.Type {
	case ctrlHandshake:
		s.handleCtrlConn(conn, dec, enc)
	case dataHandshake:
		s.handleDataConn(conn, dec, enc)
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
