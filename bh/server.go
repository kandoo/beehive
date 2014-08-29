package bh

import (
	"encoding/gob"
	"io"
	"net"

	"github.com/golang/glog"
)

type handShakeType int

const (
	ctrlHandshake handShakeType = iota
	dataHandshake
)

type hiveHandshake struct {
	Type handShakeType
}

type hiveRemoteCommand struct {
	Type routineCmdType
	BeeId
}

func (h *hive) handleCtrlConn(conn net.Conn, dec *gob.Decoder,
	enc *gob.Encoder) {

	for {
		var cmd hiveRemoteCommand
		if err := dec.Decode(&cmd); err != nil {
			glog.Errorf("Cannot decode the command: %v", err)
			return
		}

		if cmd.HiveId != h.id {
			glog.Errorf("Command is not for this hive: %+v", cmd)
			return
		}

		a, ok := h.app(cmd.AppName)
		if !ok {
			glog.Errorf("Cannot find app: %v", cmd.BeeId.AppName)
			return
		}

		switch cmd.Type {
		case createBeeCmd:
			resCh := make(chan asyncResult)
			a.qee.ctrlCh <- routineCmd{createBeeCmd, nil, resCh}
			res, err := (<-resCh).get()
			if err != nil {
				glog.Error(err)
				return
			}

			if err := enc.Encode(res.(BeeId)); err != nil {
				glog.Errorf("Cannot encode bee id: %v", err)
				return
			}

		case replaceBeeCmd:
			data := replaceBeeCmdData{}
			if err := dec.Decode(&data); err != nil {
				glog.Errorf("Cannot decode the data for replace bee command: %+v",
					err)
				return
			}

			resCh := make(chan asyncResult)
			a.qee.ctrlCh <- routineCmd{replaceBeeCmd, data, resCh}
			res, err := (<-resCh).get()
			if err != nil {
				glog.Error(err)
				return
			}

			if err := enc.Encode(res.(bee).id()); err != nil {
				glog.Errorf("Cannot encode bee id: %v", err)
				return
			}
		}
	}
}

func (h *hive) handleDataConn(conn net.Conn, dec *gob.Decoder,
	enc *gob.Encoder) {

	var to BeeId
	if err := dec.Decode(&to); err != nil {
		glog.Errorf("Cannot decode the target bee id for data conn: %+v", err)
		return
	}

	a, ok := h.app(to.AppName)
	if !ok {
		glog.Errorf("Cannot find app: %s", to.AppName)
		return
	}

	resCh := make(chan asyncResult)
	a.qee.ctrlCh <- routineCmd{findBeeCmd, to, resCh}
	res, err := (<-resCh).get()
	if err != nil {
		glog.Error(err)
		return
	}

	bee := res.(bee)
	id := bee.id()
	enc.Encode(id)
	glog.V(2).Infof("Encoded bee id: %+v", id)

	for {
		m := msg{}
		if err := dec.Decode(&m); err != nil {
			if err != io.EOF {
				glog.Errorf("Cannot decode message: %v", err)
			}
			return
		}

		glog.V(3).Infof("Received a message from peer: %+v", m)
		a.qee.dataCh <- msgAndHandler{&m, a.handlers[m.Type()]}
	}
}

func (h *hive) handleConn(conn net.Conn) {
	defer conn.Close()

	dec := gob.NewDecoder(conn)
	enc := gob.NewEncoder(conn)

	var hs hiveHandshake
	err := dec.Decode(&hs)
	if err != nil {
		glog.Errorf("Cannot decode server handshake: %+v", err)
		return
	}

	glog.V(2).Infof("Peer handshaked: %+v", hs)

	switch hs.Type {
	case ctrlHandshake:
		h.handleCtrlConn(conn, dec, enc)
	case dataHandshake:
		h.handleDataConn(conn, dec, enc)
	}
}

func (h *hive) listen() {
	var err error
	h.listener, err = net.Listen("tcp", h.config.HiveAddr)
	if err != nil {
		glog.Fatalf("Cannot start listener: %v", err)
	}
	defer h.listener.Close()

	glog.Infof("Hive listening at: %s", h.config.HiveAddr)
	for {
		c, err := h.listener.Accept()
		if err != nil {
			glog.V(2).Info("Listener closed.")
			return
		}
		glog.V(2).Infof("Accepting a new connection: %v -> %v", c.RemoteAddr(),
			c.LocalAddr())
		go h.handleConn(c)
	}
}
