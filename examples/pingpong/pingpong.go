package main

import (
	"encoding/binary"
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

func (p *pinger) Map(msg bh.Msg, ctx bh.MapContext) bh.MappedCells {
	return centralizedMappedCells
}

func (p *pinger) Rcv(msg bh.Msg, ctx bh.RcvContext) error {
	dict := ctx.Dict(PingPongDict)
	data := msg.Data()
	switch data := data.(type) {
	case ping:
		fmt.Printf("Rx Ping %d @ %v\n", data.Seq, ctx.ID())
		time.Sleep(100 * time.Millisecond)

		v, err := dict.Get("ping")
		var p ping
		if err == nil {
			p.decode(v)
		}

		if data != p {
			return fmt.Errorf("Invalid ping: %d != %d", data.Seq, p.Seq)
		}

		p.Seq += 1
		dict.Put("ping", p.encode())

		fmt.Printf("Ping stored to %v\n", p.Seq)
		fmt.Printf("Tx Pong %d @ %v\n", data.pong().Seq, ctx.ID())

		ctx.Emit(data.pong())

	case pong:
		fmt.Printf("Rx Pong %d @ %v\n", data.Seq, ctx.ID())

		time.Sleep(100 * time.Millisecond)

		dict := ctx.Dict(PingPongDict)
		v, err := dict.Get("pong")
		var p pong
		if err == nil {
			p.decode(v)
		}

		if data != p {
			return fmt.Errorf("Invalid pong: %d != %d", data.Seq, p.Seq)
		}

		p.Seq += 1
		dict.Put("pong", p.encode())
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

	pingApp := bh.NewApp("Ping", bh.AppPersistent(2))
	pingApp.Handle(pong{}, &pinger{})

	pongApp := bh.NewApp("Pong", bh.AppPersistent(2))
	pongApp.Handle(ping{}, &ponger{})

	if *shouldPing {
		bh.Emit(ping{})
	}

	if *shouldPong {
		bh.Emit(pong{})
	}

	bh.Start()
}
