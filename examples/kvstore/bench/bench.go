package main

import (
	"bufio"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"runtime/pprof"
	"time"

	bh "github.com/kandoo/beehive"
	"github.com/kandoo/beehive/Godeps/_workspace/src/golang.org/x/net/context"
	"github.com/kandoo/beehive/examples/kvstore/store"
)

var (
	replFactor = flag.Int("kv.rf", 3, "replication factor")
	buckets    = flag.Int("kv.b", 1024, "number of buckets")
	rounds     = flag.Int("kv.r", 100, "rounds of benchmark")
	numkeys    = flag.Int("kv.n", 1024, "number of keys")
	tries      = flag.Int("kv.t", 1024, "number of requests sent per key")
	get        = flag.Bool("kv.get", false, "use gets")
	cpuprofile = flag.String("kv.cpuprofile", "", "write cpu profile to file")
	quiet      = flag.Bool("kv.quiet", false, "no raft log")
	random     = flag.Bool("kv.rand", false, "whether to use random placement")
	output     = flag.String("kv.output", "bench.out", "the output file")
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
			panic(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	os.RemoveAll(bh.DefaultCfg.StatePath)

	hive := bh.NewHive()
	app := hive.NewApp("kvstore", bh.Persistent(*replFactor))
	sync := bh.NewSync(app)
	kvs := &store.KVStore{
		Sync:    sync,
		Buckets: uint64(*buckets),
	}
	app.Handle(store.Put{}, kvs)
	app.Handle(store.Get(""), kvs)
	sync.Handle(store.Put{}, kvs)
	sync.Handle(store.Get(""), kvs)
	go hive.Start()

	time.Sleep(4 * time.Minute)

	keys := make([]string, *numkeys)
	reqs := make([]interface{}, *numkeys)
	val := "val"
	for i, _ := range keys {
		keys[i] = fmt.Sprintf("%dkeys%d", i, i)
		if *get {
			reqs[i] = store.Get(keys[i])
		} else {
			reqs[i] = store.Put{Key: keys[i], Val: val}
		}
	}

	for _, k := range keys {
		hive.Emit(store.Put{Key: k, Val: val})
		sync.Process(context.Background(), store.Get(k))
	}

	ts := make([]time.Duration, *rounds)
	for i := 0; i < *rounds; i++ {
		start := time.Now()
		for j := 0; j < *tries; j++ {
			for _, r := range reqs {
				hive.Emit(r)
			}
		}
		for _, k := range keys {
			sync.Process(context.Background(), store.Get(k))
		}
		ts[i] = time.Since(start)
	}

	hive.Stop()

	f, err := os.Create(*output)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	for _, t := range ts {
		fmt.Fprintf(w, "%v\n", uint64(t))
	}
	w.Flush()
}
