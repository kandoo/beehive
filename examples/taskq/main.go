package main

import (
	"flag"
	"log"
	"os"
	"runtime/pprof"

	"github.com/kandoo/beehive"
	"github.com/kandoo/beehive/examples/taskq/server"
)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

func main() {
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	h := beehive.NewHive()
	server.RegisterTaskQ(h)
	h.Start()
}
