package main

import (
	"time"

	"github.com/soheilhy/actor/actor"
)

func main() {
	s := actor.NewStage()

	c := s.NewActor("Collector")
	p := NewPoller(1 * time.Second)
	c.Detached(p)
	c.Handle(StatResult{}, &Collector{1000000, p})
	c.Handle(SwitchJoined{}, &SwitchJoinHandler{p})

	r := s.NewActor("Router")
	r.Handle(MatrixUpdate{}, &UpdateHandler{})

	join := make(chan interface{})
	s.Start(join)
}
