package main

import (
	"github.com/soheilhy/beehive/bh"
	"github.com/soheilhy/beehive/openflow"
)

func main() {
	h := bh.NewHive()
	openflow.StartOpenFlow(h)

	ch := make(chan bool)
	h.Start(ch)
}
