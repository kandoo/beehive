package bh

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/coreos/go-etcd/etcd"
	"github.com/golang/glog"
)

// HiveJoined is emitted when a hive joins the cluster. Note that this message
// is emitted on all hives.
type HiveJoined struct {
	HiveID HiveID // The ID of the hive.
}

// HiveLeft is emitted when a hive leaves the cluster. Note that this event is
// emitted on all hives.
type HiveLeft struct {
	HiveID HiveID // The ID of the hive.
}

const (
	regPrefix    = "beehive"
	regAppDir    = "apps"
	regHiveDir   = "hives"
	regAppTTL    = 0
	regHiveTTL   = 60
	expireAction = "expire"
	lockFileName = "__lock__"
)

type registry struct {
	*etcd.Client
	hive        *hive
	prefix      string
	hiveDir     string
	hiveTTL     uint64
	appDir      string
	appTTL      uint64
	ttlCancelCh chan chan bool
	watchCmdCh  chan LocalCmd
	watchMutex  sync.Mutex
	liveHives   map[HiveID]bool
}

func (h *hive) connectToRegistry() {
	if len(h.config.RegAddrs) == 0 {
		return
	}

	// TODO(soheil): Add TLS registry.
	h.registry = registry{
		Client:  etcd.NewClient(h.config.RegAddrs),
		hive:    h,
		prefix:  regPrefix,
		hiveDir: regHiveDir,
		hiveTTL: regHiveTTL,
		appDir:  regAppDir,
		appTTL:  regAppTTL,
	}

	if ok := h.registry.SyncCluster(); !ok {
		glog.Fatalf("Cannot connect to registry nodes: %s", h.config.RegAddrs)
	}

	h.RegisterMsg(HiveJoined{})
	h.RegisterMsg(HiveLeft{})
	h.registry.registerHive()
	h.registry.startPollers()
}

func (g *registry) disconnect() {
	if !g.connected() {
		return
	}

	watchStopCh := make(chan CmdResult)
	g.watchCmdCh <- NewLocalCmd(stopCmd{}, BeeID{}, watchStopCh)
	<-watchStopCh

	cancelRes := make(chan bool)
	g.ttlCancelCh <- cancelRes
	<-cancelRes

	g.unregisterHive()
}

func (g registry) connected() bool {
	return g.Client != nil
}

func (g *registry) hiveRegKeyVal() (string, string) {
	v := string(g.hive.ID())
	return g.hivePath(v), v
}

func (g *registry) registerHive() {
	k, v := g.hiveRegKeyVal()
	if _, err := g.Create(k, v, g.hiveTTL); err != nil {
		glog.Fatalf("Error in registering hive entry: %v", err)
	}
}

func (g *registry) unregisterHive() {
	k, _ := g.hiveRegKeyVal()
	if _, err := g.Delete(k, false); err != nil {
		glog.Fatalf("Error in unregistering hive entry: %v", err)
	}
}

func (g *registry) startPollers() {
	g.ttlCancelCh = make(chan chan bool)
	go g.updateTTL()

	g.liveHives = make(map[HiveID]bool)
	g.watchCmdCh = make(chan LocalCmd)
	go g.watchHives()
}

func (g *registry) updateTTL() {
	waitTimeout := g.hiveTTL / 2
	if waitTimeout == 0 {
		waitTimeout = 1
	}

	for {
		select {
		case ch := <-g.ttlCancelCh:
			ch <- true
			return
		case <-time.After(time.Duration(waitTimeout) * time.Second):
			k, v := g.hiveRegKeyVal()
			if _, err := g.Update(k, v, g.hiveTTL); err != nil {
				glog.Fatalf("Error in updating hive entry in the registry: %v", err)
			}
			glog.V(1).Infof("Hive %s's TTL updated in registry", g.hive.ID())
		}
	}
}

func (g *registry) watchHives() {
	res, err := g.Get(g.hivePath(), false, true)
	if err != nil {
		glog.Fatalf("Cannot find the hive directory: %v", err)
	}

	for _, n := range res.Node.Nodes {
		hiveID := g.hiveIDFromPath(n.Key)
		g.AddHive(hiveID)
	}

	resCh := make(chan *etcd.Response)
	joinCh := make(chan bool)
	stopCh := make(chan bool)
	go func() {
		g.Watch(g.hivePath(), res.EtcdIndex, true, resCh, stopCh)
		joinCh <- true
	}()

	for {
		select {
		case wcmd := <-g.watchCmdCh:
			switch wcmd.Cmd.(type) {
			case stopCmd:
				stopCh <- true
				<-joinCh
				wcmd.ResCh <- CmdResult{}
				return
			}
		case res := <-resCh:
			if res == nil {
				continue
			}

			id := g.hiveIDFromPath(res.Node.Key)
			switch res.Action {
			case "create":
				if res.PrevNode == nil {
					g.AddHive(id)
				}
			case "delete":
				if res.PrevNode != nil {
					g.DelHive(id)
				}
			default:
				glog.V(2).Infof("Received an update from registry: %+v", *res)
			}
		}
	}
}

func (g *registry) AddHive(id HiveID) bool {
	g.watchMutex.Lock()
	defer g.watchMutex.Unlock()
	if g.liveHives[id] {
		return false
	}

	g.liveHives[id] = true
	g.hive.Emit(HiveJoined{id})
	return true
}

func (g *registry) DelHive(id HiveID) bool {
	g.watchMutex.Lock()
	defer g.watchMutex.Unlock()
	if !g.liveHives[id] {
		return false
	}

	delete(g.liveHives, id)

	g.hive.Emit(HiveLeft{id})
	return true
}

func (g *registry) ListHives() []HiveID {
	g.watchMutex.Lock()
	defer g.watchMutex.Unlock()
	hives := make([]HiveID, 0, len(g.liveHives))
	for h := range g.liveHives {
		hives = append(hives, h)
	}
	return hives
}

func (g registry) path(elem ...string) string {
	return g.prefix + "/" + strings.Join(elem, "/")
}

func (g registry) appPath(elem ...string) string {
	return g.prefix + "/" + g.appDir + "/" + strings.Join(elem, "/")
}

func (g registry) hivePath(elem ...string) string {
	return g.prefix + "/" + g.hiveDir + "/" + strings.Join(elem, "/")
}

func (g registry) hiveIDFromPath(path string) HiveID {
	prefixLen := len(g.hivePath()) + 1
	return HiveID(path[prefixLen:])
}

func (g registry) lockApp(id BeeID) error {
	// TODO(soheil): For lock and unlock we can use etcd indices but
	// v.Temp might be changed by the app. Check this and fix it if possible.
	k := g.appPath(string(id.AppName), lockFileName)

	for {
		// FIXME(soheil): This is very dangerous when the bee dies before unlock.
		_, err := g.Create(k, string(id.Bytes()), g.appTTL)
		if err == nil {
			return nil
		}

		_, err = g.Watch(k, 0, false, nil, nil)
		if err != nil {
			return err
		}
	}
}

func (g registry) unlockApp(id BeeID) error {
	k := g.appPath(string(id.AppName), lockFileName)
	res, err := g.Get(k, false, false)
	if err != nil {
		return err
	}

	v := BeeIDFromBytes([]byte(res.Node.Value))
	if id != v {
		return fmt.Errorf("Unlocking someone else's lock: %v, %v", id, v)
	}

	_, err = g.Delete(k, false)
	if err != nil {
		return err
	}

	return nil
}

func (g registry) set(c BeeColony, ms MappedCells) BeeID {
	err := g.lockApp(c.Master)
	if err != nil {
		glog.Fatalf("Cannot lock app %v: %v", c.Master, err)
	}

	defer func() {
		err := g.unlockApp(c.Master)
		if err != nil {
			glog.Fatalf("Cannot unlock app %v: %v", c.Master, err)
		}
	}()

	sort.Sort(ms)

	v, err := c.Bytes()
	if err != nil {
		glog.Fatalf("Cannot serialize BeeColony: %s", err)
	}

	for _, dk := range ms {
		k := g.appPath(string(c.Master.AppName), string(dk.Dict), string(dk.Key))
		_, err := g.Set(k, string(v), g.appTTL)
		if err != nil {
			glog.Fatalf("Cannot set bee: %+v", k)
		}
	}
	return c.Master
}

func (g registry) storeOrGet(c BeeColony, ms MappedCells) BeeID {
	err := g.lockApp(c.Master)
	if err != nil {
		glog.Fatalf("Cannot lock app %v: %v", c.Master, err)
	}

	defer func() {
		err := g.unlockApp(c.Master)
		if err != nil {
			glog.Fatalf("Cannot unlock app %v: %v", c.Master, err)
		}
	}()

	sort.Sort(ms)

	v, err := c.Bytes()
	if err != nil {
		glog.Fatalf("Cannot serialize BeeColony: %s", err)
	}

	validate := false
	for _, dk := range ms {
		k := g.appPath(string(c.Master.AppName), string(dk.Dict), string(dk.Key))
		res, err := g.Get(k, false, false)
		if err != nil {
			continue
		}

		entry, err := BeeColonyFromBytes([]byte(res.Node.Value))
		if err != nil {
			glog.Fatalf("Cannot decode a BeeColony: %s", err)
		}

		if entry.Eq(c) {
			continue
		}

		if validate {
			glog.Fatalf("Incosistencies for bee %v: %v != %v", c.Master, c, entry)
		}

		c = entry
		v = []byte(res.Node.Value)
		validate = true
	}

	for _, dk := range ms {
		k := g.appPath(string(c.Master.AppName), string(dk.Dict), string(dk.Key))
		g.Create(k, string(v), g.appTTL)
	}

	return c.Master
}

func (g registry) compareAndSet(oldC BeeColony, newC BeeColony,
	ms MappedCells) (BeeColony, error) {

}
