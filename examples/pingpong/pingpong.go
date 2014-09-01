package main

import (
	"encoding/binary"
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

type Pxng struct {
	Seq int
}

func (p *Pxng) decode(b []byte) {
	p.Seq = int(binary.LittleEndian.Uint64(b))
}

func (p *Pxng) encode() []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(p.Seq))
	return b
}

type ping struct {
	Pxng
}

type pong struct {
	Pxng
}

func (p ping) pong() pong {
	return pong{Pxng{p.Seq + 1}}
}

func (p pong) ping() ping {
	return ping{Pxng{p.Seq + 1}}
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
		fmt.Printf("Rx Ping %d\n", data.Seq)
		time.Sleep(100 * time.Millisecond)

		v, err := dict.Get("ping")
		var p ping
		if err == nil {
			p.decode(v)
		}

		if data != p {
			glog.Fatalf("Invalid ping: %d != %d", data, p.Seq)
		}

		p.Seq += 1
		dict.Put("ping", p.encode())

		fmt.Printf("Tx Pong %d\n", data.Seq)
		ctx.Emit(data.pong())

	case pong:
		fmt.Printf("Rx Pong %d\n", data.Seq)
		time.Sleep(100 * time.Millisecond)

		dict := ctx.Dict(PingPongDict)
		v, err := dict.Get("pong")
		var p pong
		if err == nil {
			p.decode(v)
		}

		if data != p {
			glog.Fatalf("Invalid pong: %d != %d", data, p)
		}

		p.Seq += 1
		dict.Put("pong", p.encode())

		fmt.Printf("Tx Ping %d\n", data.Seq)
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
	pingAtor.Handle(pong{}, &pinger{})

	pongAtor := h.NewApp("Pong")
	pongAtor.Handle(ping{}, &ponger{})

	if *shouldPing {
		h.Emit(ping{})
	}

	if *shouldPong {
		h.Emit(pong{})
	}

	join := make(chan bool)
	go h.Start(join)
	<-join
}
