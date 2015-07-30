package main

import (
	"flag"
	"fmt"
	"time"

	bh "github.com/kandoo/beehive"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/golang/glog"
)

const (
	maxSpike = 10000
)

var (
	elephantProb float64
)

func createHive(config bh.HiveConfig, minDriver, maxDriver int,
	minCol, maxCol int, stickyCollector bool, lockRouter bool, joinCh chan bool) {
	h := bh.NewHiveWithConfig(config)

	cOps := []bh.AppOption{}
	if stickyCollector {
		cOps = append(cOps, bh.Sticky())
	}
	c := h.NewApp("Collector", cOps...)
	p := NewPoller(1 * time.Second)
	c.Detached(p)
	c.Handle(StatResult{}, &Collector{uint64(maxSpike * (1 - elephantProb)), p})
	c.Handle(SwitchJoined{}, &SwitchJoinHandler{p})

	r := h.NewApp("Router", bh.Sticky())
	r.Handle(MatrixUpdate{}, &UpdateHandler{})

	d := h.NewApp("Driver", bh.Sticky())
	driver := NewDriver(minDriver, maxDriver-minDriver)
	d.Handle(StatQuery{}, driver)
	d.Handle(FlowMod{}, driver)

	if lockRouter {
		h.Emit(MatrixUpdate{})
	}

	if maxDriver != minDriver {
		glog.Infof("Running driver from %d to %d\n", minDriver, maxDriver-1)
		d.Detached(driver)
		for i := minDriver; i < maxDriver; i++ {
			h.Emit(StatQuery{Switch(i)})
		}
	}

	if maxCol != minCol {
		glog.Infof("Running collector from %d to %d\n", minCol, maxCol-1)
		for i := minCol; i < maxCol; i++ {
			h.Emit(SwitchJoined{Switch(i)})
		}
	}

	h.RegisterMsg(SwitchStats{})
	go func() {
		h.Start()
		<-joinCh
	}()
}

func main() {
	flag.Float64Var(&elephantProb, "p", 0.1,
		"The probability of an elephant flow.")
	nswitches := flag.Int("nswitches", 4, "Number of switches.")
	nhives := flag.Int("nhives", 4, "Number of hives.")
	stickyCol := flag.Bool("stickycollectors", false,
		"Whether collectors are sticky.")
	centCol := flag.Bool("centralized", false,
		"Whether to centralize the collectors")
	flag.Parse()

	lAddr := "127.0.0.1:%d"
	port := 7777
	driverPerHive := *nswitches / *nhives
	var collectorPerHive int
	if *centCol {
		collectorPerHive = 0
	} else {
		collectorPerHive = *nswitches / *nhives
	}

	joinChannels := make([]chan bool, 0)

	config := bh.DefaultCfg
	for h := 0; h < *nhives; h++ {
		config.Addr = fmt.Sprintf(lAddr, port)
		port++

		jCh := make(chan bool)
		joinChannels = append(joinChannels, jCh)

		if *centCol && h == 0 {
			createHive(config, h*driverPerHive, (h+1)*driverPerHive,
				0, *nswitches, *stickyCol, true, jCh)
			time.Sleep(1 * time.Second)
			continue
		}

		createHive(config, h*driverPerHive, (h+1)*driverPerHive,
			h*collectorPerHive, (h+1)*collectorPerHive, *stickyCol, false, jCh)

	}

	for _, ch := range joinChannels {
		<-ch
	}
}
