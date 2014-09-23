package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"time"

	"github.com/soheilhy/beehive/bh"
	"github.com/soheilhy/beehive/examples/routing"
)

// emitPod emits discovery messages for the links of a pod p in a k-ary fat-tree
// network. Note that 1 <= p <= k.
func emitPod(p int, k int) error {
	if p < 1 || p > k {
		return fmt.Errorf("No pod #%d in a %d-ary fat-tree", p, k)
	}

	// Emit internal links in the pod.
	for e := 1; e <= k/2; e++ {
		edgeN := routing.Node{
			ID: fmt.Sprintf("E-%d-%d", p, e),
		}
		for h := 1; h <= k/2; h++ {
			hostN := routing.Node{
				ID:      fmt.Sprintf("H-%d-%d-%d", p, e, h),
				Endhost: true,
			}
			bh.Emit(routing.Discovery(routing.Edge{
				From: edgeN,
				To:   hostN,
			}))
			bh.Emit(routing.Discovery(routing.Edge{
				From: hostN,
				To:   edgeN,
			}))
		}
		for a := 1; a <= k/2; a++ {
			aggrN := routing.Node{
				ID: fmt.Sprintf("A-%d-%d", p, a),
			}
			bh.Emit(routing.Discovery(routing.Edge{
				From: edgeN,
				To:   aggrN,
			}))
			bh.Emit(routing.Discovery(routing.Edge{
				From: aggrN,
				To:   edgeN,
			}))
			fmt.Printf("%v <-> %v\n", aggrN, edgeN)
		}
	}

	// Emit uplinks to core switches.
	for a := 1; a <= k/2; a++ {
		aggrN := routing.Node{
			ID: fmt.Sprintf("A-%d-%d", p, a),
		}
		for c := k/2*(a-1) + 1; c <= k/2*a; c++ {
			coreN := routing.Node{
				ID: fmt.Sprintf("C-%d", c),
			}
			bh.Emit(routing.Discovery(routing.Edge{
				From: aggrN,
				To:   coreN,
			}))
			bh.Emit(routing.Discovery(routing.Edge{
				From: coreN,
				To:   aggrN,
			}))
			fmt.Printf("%v <-> %v\n", coreN, aggrN)
		}
	}

	return nil
}

var from = flag.Int("from", 1, "First pod in [1..k]")
var to = flag.Int("to", 4, "Last pod in [1..k]")
var k = flag.Int("k", 4, "Number of ports of switches (must be even)")
var epoc = flag.Duration("epoc", 100*time.Millisecond,
	"The duration between route advertisement epocs.")
var idleTimeout = flag.Duration("idletimeout", 60*time.Second,
	"If the router was idle for this duration, the program is killed.")
var cpuProfile = flag.String("cpuprofile", "", "write cpu profile to file")

func main() {
	flag.Parse()

	if *k%2 != 0 || *from < 1 || *to < 1 || *k < *from || *k < *to {
		log.Fatal("Invalid parameters", *from, *to, *k)
	}

	flag.Parse()
	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	routing.InstallRouting(*epoc)

	chrono := bh.NewApp("Chrono")
	ch := make(chan bool, 1024)
	rcvF := func(msg bh.Msg, ctx bh.RcvContext) error {
		ch <- true
		return nil
	}
	mapF := func(msg bh.Msg, ctx bh.MapContext) bh.MappedCells {
		return ctx.LocalCells()
	}
	chrono.HandleFunc(routing.Advertisement{}, mapF, rcvF)

	start := time.Now()
	for p := *from; p <= *to; p++ {
		go func(p int) {
			if err := emitPod(p, *k); err != nil {
				panic(err)
			}
		}(p)
	}
	go bh.Start()

	finish := time.Now()
	for {
		select {
		case <-ch:
			finish = time.Now()
		case <-time.After(*idleTimeout):
			log.Fatalf("Took %v (%v-%v)", finish.Sub(start), start, finish)
		}
	}
}
