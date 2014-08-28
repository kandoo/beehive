package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/golang/glog"
	"github.com/soheilhy/beehive/bh"
)

const (
	maxSpike = 10000
)

var (
	elephantProb float64
)

func createStage(config bh.StageConfig, minDriver, maxDriver int,
	minCol, maxCol int, stickyCollector bool, lockRouter bool,
	joinCh chan interface{}) {
	s := bh.NewStageWithConfig(config)

	c := s.NewActor("Collector")
	p := NewPoller(1 * time.Second)
	c.Detached(p)
	c.Handle(StatResult{}, &Collector{uint64(maxSpike * (1 - elephantProb)), p})
	c.Handle(SwitchJoined{}, &SwitchJoinHandler{p})
	c.SetSticky(stickyCollector)

	r := s.NewActor("Router")
	r.Handle(MatrixUpdate{}, &UpdateHandler{})
	r.SetSticky(true)

	d := s.NewActor("Driver")
	driver := NewDriver(minDriver, maxDriver-minDriver)
	d.Handle(StatQuery{}, driver)
	d.Handle(FlowMod{}, driver)
	d.SetSticky(true)

	if lockRouter {
		s.Emit(MatrixUpdate{})
	}

	if maxDriver != minDriver {
		glog.Infof("Running driver from %d to %d\n", minDriver, maxDriver-1)
		d.Detached(driver)
		for i := minDriver; i < maxDriver; i++ {
			s.Emit(StatQuery{Switch(i)})
		}
	}

	if maxCol != minCol {
		glog.Infof("Running collector from %d to %d\n", minCol, maxCol-1)
		for i := minCol; i < maxCol; i++ {
			s.Emit(SwitchJoined{Switch(i)})
		}
	}

	s.RegisterMsg(SwitchStats{})
	go s.Start(joinCh)
}

func main() {
	flag.Float64Var(&elephantProb, "p", 0.1,
		"The probability of an elephant flow.")
	nswitches := flag.Int("nswitches", 4, "Number of switches.")
	nstages := flag.Int("nstages", 4, "Number of stages.")
	stickyCol := flag.Bool("stickycollectors", false,
		"Whether collectors are sticky.")
	centCol := flag.Bool("centralized", false,
		"Whether to centralize the collectors")
	flag.Parse()

	lAddr := "127.0.0.1:%d"
	port := 7777
	driverPerStage := *nswitches / *nstages
	var collectorPerStage int
	if *centCol {
		collectorPerStage = 0
	} else {
		collectorPerStage = *nswitches / *nstages
	}

	joinChannels := make([]chan interface{}, 0)

	config := bh.DefaultCfg
	for s := 0; s < *nstages; s++ {
		config.StageAddr = fmt.Sprintf(lAddr, port)
		port++

		jCh := make(chan interface{})
		joinChannels = append(joinChannels, jCh)

		if *centCol && s == 0 {
			createStage(config, s*driverPerStage, (s+1)*driverPerStage,
				0, *nswitches, *stickyCol, true, jCh)
			time.Sleep(1 * time.Second)
			continue
		}

		createStage(config, s*driverPerStage, (s+1)*driverPerStage,
			s*collectorPerStage, (s+1)*collectorPerStage, *stickyCol, false, jCh)

	}

	for _, ch := range joinChannels {
		<-ch
	}
}
