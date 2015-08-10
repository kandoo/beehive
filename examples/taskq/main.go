package main

import (
	"flag"
	"os"
	"runtime/pprof"

	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/golang/glog"

	"github.com/kandoo/beehive"
	"github.com/kandoo/beehive/examples/taskq/server"
)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

func main() {
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			glog.Error("cannot register cpu profile: %v", err)
			os.Exit(-1)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	h := beehive.NewHive()
	if err := server.RegisterTaskQ(h); err != nil {
		glog.Errorf("cannot register taskq: %v", err)
		os.Exit(-1)
	}
	h.Start()
}
