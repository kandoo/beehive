package beehive

import (
	"encoding/gob"
	"os"
	"path"
	"time"

	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/golang/glog"
	"github.com/kandoo/beehive/raft"
)

type HiveInfo raft.NodeInfo

type hiveMeta struct {
	Hive  HiveInfo
	Peers map[uint64]HiveInfo
}

func peersInfo(addrs []string) map[uint64]HiveInfo {
	if len(addrs) == 0 {
		return nil
	}

	ch := make(chan []HiveInfo, len(addrs))
	for _, a := range addrs {
		go func(a string) {
			s, err := getHiveState(a)
			if err != nil {
				glog.Errorf("cannot communicate with %v: %v", a, err)
				return
			}
			ch <- s.Peers
		}(a)
	}

	// Return the first one.
	hives := <-ch
	glog.V(2).Infof("found live hives: %v", hives)
	infos := make(map[uint64]HiveInfo)
	for _, h := range hives {
		infos[h.ID] = h
	}
	return infos
}

func hiveIDFromPeers(addr string, paddrs []string) uint64 {
	if len(paddrs) == 0 {
		return 1
	}

	ch := make(chan uint64, len(paddrs))
	for _, paddr := range paddrs {
		glog.Infof("requesting hive ID from %v", paddr)
		go func(paddr string) {
			c, err := newRPCClient(paddr)
			if err != nil {
				glog.Error(err)
				return
			}

			id, err := c.sendCmd(cmd{Data: cmdNewHiveID{Addr: addr}})
			if err != nil {
				glog.Error(err)
				return
			}

			_, err = c.sendCmd(cmd{
				Data: cmdAddHive{
					Info: raft.NodeInfo{
						ID:   id.(uint64),
						Addr: addr,
					},
				},
			})
			if err != nil {
				glog.Error(err)
				return
			}
			ch <- id.(uint64)
		}(paddr)
		select {
		case id := <-ch:
			return id
		case <-time.After(1 * time.Second):
			glog.Infof("cannot get id from %v", paddr)
			continue
		}
	}

	glog.Fatalf("cannot get a new hive ID from peers")
	return 1
}

func meta(cfg HiveConfig) hiveMeta {
	m := hiveMeta{}

	var dec *gob.Decoder
	metapath := path.Join(cfg.StatePath, "meta")
	f, err := os.Open(metapath)
	if err != nil {
		// TODO(soheil): We should also update our peer addresses when we have an
		// existing meta.
		m.Peers = peersInfo(cfg.PeerAddrs)
		m.Hive.Addr = cfg.Addr
		if len(cfg.PeerAddrs) == 0 {
			// The initial ID is 1. There is no raft node up yet to allocate an ID. So
			// we must do this when the hive starts.
			m.Hive.ID = 1
			goto save
		}

		m.Hive.ID = hiveIDFromPeers(cfg.Addr, cfg.PeerAddrs)
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
		glog.Fatalf("cannot open meta file: %v", err)
	}

	enc := gob.NewEncoder(f)
	if err := enc.Encode(&m); err != nil {
		glog.Fatalf("cannot encode meta: %v", err)
	}

	f.Close()
}
