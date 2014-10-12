package bh

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

var addrForTest int = 32771

func hiveAddrsForTest(n int) []string {
	addrs := make([]string, 0, n)
	for i := 0; i < n; i++ {
		addrs = append(addrs, fmt.Sprintf("127.0.0.1:%d", addrForTest+i))
	}
	addrForTest += n
	return addrs
}

type hiveJoinedHandler struct {
	joined chan bool
}

func (h *hiveJoinedHandler) Rcv(msg Msg, ctx RcvContext) error {
	return nil
}

func (h *hiveJoinedHandler) Map(msg Msg, ctx MapContext) MappedCells {
	h.joined <- true
	return nil
}

func maybeSkipRegistryTest(t *testing.T) {
	if len(DefaultCfg.RegAddrs) != 0 {
		return
	}

	t.Skip("Registry tests run only when the hive is connected to registry.")
}

func hiveWithAddressForTest(addr string, t *testing.T) *hive {
	cfg := DefaultCfg
	cfg.HiveAddr = addr
	return NewHiveWithConfig(cfg).(*hive)
}

func startHivesForReplicationTest(t *testing.T, addrs []string,
	preStart func(h Hive)) []Hive {

	maybeSkipRegistryTest(t)

	hiveJoinedCh := make(chan bool, len(addrs)*2)
	hives := make([]Hive, len(addrs))
	for i, a := range addrs {
		hives[i] = hiveWithAddressForTest(a, t)
		hives[i].NewApp("joined").Handle(HiveJoined{}, &hiveJoinedHandler{
			joined: hiveJoinedCh,
		})
		preStart(hives[i])
		go hives[i].Start()
	}

	for _ = range addrs {
		for _ = range addrs {
			<-hiveJoinedCh
		}
	}

	// To make sure the replication strategy has processed hive joins.
	time.Sleep(100 * time.Millisecond)

	return hives
}

func stopHives(hives ...Hive) {
	wg := sync.WaitGroup{}
	for i := range hives {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			hives[i].Stop()
		}(i)
	}
	wg.Wait()
}
