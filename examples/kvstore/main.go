package main

import (
	"flag"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"runtime/pprof"
	"time"

	bh "github.com/kandoo/beehive"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/golang/glog"
	"github.com/kandoo/beehive/examples/kvstore/store"
)

var (
	replFactor = flag.Int("kv.rf", 3, "replication factor")
	buckets    = flag.Int("kv.b", 1024, "number of buckets")
	cpuprofile = flag.String("kv.cpuprofile", "", "write cpu profile to file")
	quiet      = flag.Bool("kv.quiet", false, "no raft log")
	random     = flag.Bool("kv.rand", false, "whether to use random placement")
)

func main() {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())
	if *quiet {
		log.SetOutput(ioutil.Discard)
	}

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			glog.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	opts := []bh.AppOption{bh.Persistent(*replFactor)}
	if *random {
		rp := bh.RandomPlacement{
			Rand: rand.New(rand.NewSource(time.Now().UnixNano())),
		}
		opts = append(opts, bh.WithPlacement(rp))
	}
	a := bh.NewApp("kvstore", opts...)
	s := bh.NewSync(a)
	kv := &store.KVStore{
		Sync:    s,
		Buckets: uint64(*buckets),
	}
	s.Handle(store.Put{}, kv)
	s.Handle(store.Get(""), kv)
	s.Handle(store.Del(""), kv)
	a.HandleHTTP("/{key}", kv)

	bh.Start()
}
