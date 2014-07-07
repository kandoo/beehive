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

	d := s.NewActor("Driver")
	driver := NewDriver(0, 10)
	d.Detached(driver)
	d.Handle(StatQuery{}, driver)

	join := make(chan interface{})
	s.Start(join)
}
