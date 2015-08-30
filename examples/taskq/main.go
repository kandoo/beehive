package main

import (
	"flag"
	"math/rand"
	"os"
	"time"

	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/golang/glog"

	"github.com/kandoo/beehive"
	"github.com/kandoo/beehive/examples/taskq/server"
)

func main() {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())

	h := beehive.NewHive()
	if err := server.RegisterTaskQ(h); err != nil {
		glog.Errorf("cannot register taskq: %v", err)
		os.Exit(-1)
	}
	h.Start()
}
