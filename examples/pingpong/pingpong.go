package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/golang/glog"
	"github.com/soheilhy/beehive/bh"
)

const (
	PingPongDict = "PingPong"
)

var centralizedMapSet = bh.MapSet{{PingPongDict, "0"}}

type ping int
type pong int

func (p ping) pong() pong {
	return pong(p + 1)
}

func (p pong) ping() ping {
	return ping(p + 1)
}

type pinger struct{}

func (p *pinger) Map(msg bh.Msg, ctx bh.Context) bh.MapSet {
	return centralizedMapSet
}

func (p *pinger) Recv(msg bh.Msg, ctx bh.RecvContext) {
	dict := ctx.Dict(PingPongDict)
	data := msg.Data()
	switch data := data.(type) {
	case ping:
		fmt.Printf("Ping %d\n", data)
		time.Sleep(100 * time.Millisecond)

		v, ok := dict.Get("ping")
		if !ok {
			v = ping(0)
		}
		if data != v.(ping) {
			glog.Fatalf("Invalid ping: %d != %d", data, v)
		}
		dict.Set("ping", data+1)

		ctx.Emit(data.pong())

	case pong:
		fmt.Printf("Pong %d\n", data)
		time.Sleep(100 * time.Millisecond)

		dict := ctx.Dict(PingPongDict)
		v, ok := dict.Get("pong")
		if !ok {
			v = pong(0)
		}
		if data != v.(pong) {
			glog.Fatalf("Invalid pong: %d != %d", data, v)
		}
		dict.Set("pong", data+1)

		ctx.Emit(data.ping())
	}
}

type ponger struct {
	pinger
}

func main() {
	shouldPing := flag.Bool("ping", false, "Whether to ping.")
	shouldPong := flag.Bool("pong", false, "Whether to pong.")

	s := bh.NewStage()

	pingAtor := s.NewActor("Ping")
	pingAtor.Handle(pong(0), &pinger{})

	pongAtor := s.NewActor("Pong")
	pongAtor.Handle(ping(0), &ponger{})

	if *shouldPing {
		s.Emit(ping(0))
	}

	if *shouldPong {
		s.Emit(pong(0))
	}

	join := make(chan interface{})
	go s.Start(join)
	<-join
}
