package main

import (
	"flag"
	"fmt"
	"time"

	bh "github.com/kandoo/beehive"
)

const (
	PingPongDict = "PingPong"
)

var centralizedMappedCells = bh.MappedCells{{PingPongDict, "0"}}

type Pxng struct {
	Seq int
}

type ping struct {
	Pxng
}

type pong struct {
	Pxng
}

func (p ping) pong() pong {
	return pong{Pxng{p.Seq}}
}

func (p pong) ping() ping {
	return ping{Pxng{p.Seq + 1}}
}

type pinger struct{}

func (p *pinger) Map(msg bh.Msg, ctx bh.MapContext) bh.MappedCells {
	return centralizedMappedCells
}

func (p *pinger) Rcv(msg bh.Msg, ctx bh.RcvContext) error {
	dict := ctx.Dict(PingPongDict)
	data := msg.Data()
	switch data := data.(type) {
	case ping:
		fmt.Printf("Rx Ping %d %v->%v\n", data.Seq, msg.From(), ctx.ID())
		time.Sleep(300 * time.Millisecond)

		v, err := dict.Get("ping")
		var p ping
		if err == nil {
			p = v.(ping)
		}

		if data != p {
			return fmt.Errorf("Invalid ping: ping=%d, want=%d", data.Seq, p.Seq)
		}

		p.Seq += 1
		dict.Put("ping", p)

		fmt.Printf("Ping stored to %v\n", p.Seq)

		if !msg.NoReply() {
			fmt.Printf("Tx Pong %d @ %v\n", data.pong().Seq, ctx.ID())
			ctx.Emit(data.pong())
		}

	case pong:
		fmt.Printf("Rx Pong %d %v->%v\n", data.Seq, msg.From(), ctx.ID())

		time.Sleep(300 * time.Millisecond)

		dict := ctx.Dict(PingPongDict)
		v, err := dict.Get("pong")
		var p pong
		if err == nil {
			p = v.(pong)
		}

		if data != p {
			return fmt.Errorf("Invalid pong: pong=%d, want=%d", data.Seq, p.Seq)
		}

		p.Seq += 1
		dict.Put("pong", p)
		fmt.Printf("Pong stored to %v\n", p.Seq)

		fmt.Printf("Tx Ping %d @ %v\n", data.ping().Seq, ctx.ID())
		ctx.Emit(data.ping())
	}
	return nil
}

type ponger struct {
	pinger
}

func main() {
	shouldPing := flag.Bool("ping", false, "Whether to ping.")
	shouldPong := flag.Bool("pong", false, "Whether to pong.")

	pingApp := bh.NewApp("Ping", bh.Persistent(2))
	pingApp.Handle(pong{}, &pinger{})

	pongApp := bh.NewApp("Pong", bh.Persistent(2))
	pongApp.Handle(ping{}, &ponger{})

	if *shouldPing {
		bh.Emit(ping{})
	}

	if *shouldPong {
		bh.Emit(pong{})
	}

	bh.Start()
}
