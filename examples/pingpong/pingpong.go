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

func (p *pinger) Map(msg bh.Msg, ctx bh.MapContext) bh.MapSet {
	return centralizedMapSet
}

func (p *pinger) Rcv(msg bh.Msg, ctx bh.RcvContext) {
	dict := ctx.Dict(PingPongDict)
	data := msg.Data()
	switch data := data.(type) {
	case ping:
		fmt.Printf("Ping %d\n", data)
		time.Sleep(100 * time.Millisecond)

		v, err := dict.Get("ping")
		if err != nil {
			v = ping(0)
		}
		if data != v.(ping) {
			glog.Fatalf("Invalid ping: %d != %d", data, v)
		}
		dict.Put("ping", data+1)

		ctx.Emit(data.pong())

	case pong:
		fmt.Printf("Pong %d\n", data)
		time.Sleep(100 * time.Millisecond)

		dict := ctx.Dict(PingPongDict)
		v, err := dict.Get("pong")
		if err != nil {
			v = pong(0)
		}
		if data != v.(pong) {
			glog.Fatalf("Invalid pong: %d != %d", data, v)
		}
		dict.Put("pong", data+1)

		ctx.Emit(data.ping())
	}
}

type ponger struct {
	pinger
}

func main() {
	shouldPing := flag.Bool("ping", false, "Whether to ping.")
	shouldPong := flag.Bool("pong", false, "Whether to pong.")

	h := bh.NewHive()

	pingAtor := h.NewApp("Ping")
	pingAtor.Handle(pong(0), &pinger{})

	pongAtor := h.NewApp("Pong")
	pongAtor.Handle(ping(0), &ponger{})

	if *shouldPing {
		h.Emit(ping(0))
	}

	if *shouldPong {
		h.Emit(pong(0))
	}

	join := make(chan interface{})
	go h.Start(join)
	<-join
}
