package actor

import (
	"encoding/gob"
	"fmt"
	"net"

	"github.com/golang/glog"
)

type handlerAndDataCh struct {
	dataCh  chan msgAndHandler
	handler Handler
}

func (s *stage) handleConn(conn net.Conn) {
	dec := gob.NewDecoder(conn)
	enc := gob.NewEncoder(conn)

	var id ReceiverId
	dec.Decode(&id)

	//var dataCh chan msgAndHandler
	//handlers := make(map[MsgType][]Handler)

	enc.Encode(true)

	for {
		//m := broadcastMsg{}
		//if err := dec.Decode(&m); err != nil {
		//glog.Errorf("Cannot decode message: %v", err)
		//return
		//}

		//hs, ok := handlers[m.Type()]
		//if !ok {
		//hs := []Handler{}
		//for _, mh := range s.mappers[m.Type()] {
		//if mh.mapper.ctx.actor.Name() == id.ActorName {
		//hs = append(hs, mh.handler)
		//}
		//}
		//handlers[m.Type()] = hs
		//}

		//for _, h := range hs {
		//dataCh <- msgAndHandler{m, h}
		//}
	}
}

func (s *stage) listen() {
	fmt.Println("start")
	l, err := net.Listen("tcp", ":8080")
	if err != nil {
		glog.Fatal("Cannot start listener: %v", err)
	}

	for {
		c, err := l.Accept()
		if err != nil {
			glog.Errorf("Error in accept %s", err)
			continue
		}
		go s.handleConn(c)
	}
}
