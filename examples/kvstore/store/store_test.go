package store

import (
	"fmt"
	"os"
	"testing"

	"github.com/kandoo/beehive/Godeps/_workspace/src/golang.org/x/net/context"

	bh "github.com/kandoo/beehive"
)

const (
	bees = 64
)

func BenchmarkThroughput(b *testing.B) {
	defer os.RemoveAll(bh.DefaultCfg.StatePath)

	hive := bh.NewHive()

	if b.N < 1024 {
		return
	}

	b.StopTimer()
	app := hive.NewApp("kvstore", bh.Persistent(1))
	sync := bh.NewSync(app)
	store := &KVStore{
		Sync:    sync,
		Buckets: bees,
	}
	app.Handle(Put{}, store)
	sync.Handle(Put{}, store)
	sync.Handle(Get(""), store)
	go hive.Start()

	keys := make([]string, 64)
	for i, _ := range keys {
		keys[i] = fmt.Sprintf("%dkeys%d", i, i)
	}
	val := []byte("val")

	for _, k := range keys {
		hive.Emit(Put{Key: k, Val: val})
		sync.Process(context.Background(), Get(k))
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		for _, k := range keys {
			hive.Emit(Put{Key: k, Val: val})
			i++
		}
	}

	for _, k := range keys {
		sync.Process(context.Background(), Get(k))
	}
	b.StopTimer()
	hive.Stop()
}
