package beehive

import (
	"encoding/gob"
	"os"
	"path"
	"time"

	"github.com/golang/glog"
)

type hiveMeta struct {
	Hive  HiveInfo
	Peers []HiveInfo
}

func peersInfo(addrs []string) []HiveInfo {
	if len(addrs) == 0 {
		return []HiveInfo{}
	}

	ch := make(chan []HiveInfo, len(addrs))

	for _, a := range addrs {
		go func(a string) {
			p := newProxyWithAddr(a)
			if d, err := p.sendCmd(&cmd{Data: cmdLiveHives{}}); err == nil {
				ch <- d.([]HiveInfo)
			}
		}(a)
	}

	// Return the first one that returns.
	return <-ch
}

func hiveIDFromPeers(addrs []string) uint64 {
	if len(addrs) == 0 {
		return 1
	}

	ch := make(chan uint64, len(addrs))
	for _, a := range addrs {
		glog.V(2).Infof("Requesting hive ID from %v", a)
		go func(a string) {
			p := newProxyWithAddr(a)
			if d, err := p.sendCmd(&cmd{Data: cmdCreateHiveID{}}); err == nil {
				ch <- d.(uint64)
			}
		}(a)
		select {
		case id := <-ch:
			return id
		case <-time.After(300 * time.Millisecond):
			continue
		}
	}

	glog.Fatalf("Cannot get a new hive ID from peers")
	return 1
}

func meta(cfg HiveConfig) hiveMeta {
	m := hiveMeta{
		Peers: peersInfo(cfg.PeerAddrs),
	}

	var dec *gob.Decoder
	metapath := path.Join(cfg.StatePath, "meta")
	f, err := os.Open(metapath)
	if err != nil {
		m.Hive.Addr = cfg.Addr
		if len(cfg.PeerAddrs) == 0 {
			// The initial ID is 1. There is no raft node up yet to allocate an ID. So
			// we must do this when the hive starts.
			m.Hive.ID = 1
			goto save
		}

		m.Hive.ID = hiveIDFromPeers(cfg.PeerAddrs)
		goto save
	}

	dec = gob.NewDecoder(f)
	if err = dec.Decode(&m); err != nil {
		glog.Fatalf("Cannot decode meta: %v", err)
	}
	m.Hive.Addr = cfg.Addr
	f.Close()

save:
	saveMeta(m, cfg)
	return m
}

func saveMeta(m hiveMeta, cfg HiveConfig) {
	metafile := path.Join(cfg.StatePath, "meta")
	f, err := os.OpenFile(metafile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0700)
	if err != nil {
		glog.Fatalf("Cannot open meta file: %v", err)
	}

	enc := gob.NewEncoder(f)
	if err := enc.Encode(&m); err != nil {
		glog.Fatalf("Cannot encode meta: %v", err)
	}

	f.Close()
}
