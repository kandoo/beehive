package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/soheilhy/actor/actor"
)

var centralizedMapSet = actor.MapSet{{"PingPong", actor.Key("0")}}

type ping int
type pong int

func (p ping) pong() pong {
	return pong(p + 1)
}

func (p pong) ping() ping {
	return ping(p + 1)
}

type pinger struct{}

func (p *pinger) Map(msg actor.Msg, ctx actor.Context) actor.MapSet {
	return centralizedMapSet
}

var first = true

func (p *pinger) Recv(msg actor.Msg, ctx actor.RecvContext) {
	d := msg.Data()
	switch d := d.(type) {
	case ping:
		fmt.Printf("Ping %d\n", d)
		if first {
			time.Sleep(1 * time.Second)
		}
		ctx.Emit(d.pong())
	case pong:
		fmt.Printf("Pong %d\n", d)
		if first {
			time.Sleep(1 * time.Second)
		}
		ctx.Emit(d.ping())
	}
	first = false
}

type ponger struct {
	pinger
}

func main() {
	shouldPing := flag.Bool("ping", false, "Whether to ping.")
	shouldPong := flag.Bool("pong", false, "Whether to pong.")

	s := actor.NewStage()

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
