package main

import (
	"github.com/soheilhy/beehive/bh"
	"github.com/soheilhy/beehive/openflow"
)

func main() {
	h := bh.NewHive()
	cfg := openflow.OFConfig{
		Proto: "tcp",
		Addr:  "192.168.0.2:6633",
	}
	openflow.StartOpenFlowWithConfig(h, cfg)

	ch := make(chan bool)
	h.Start(ch)
}
