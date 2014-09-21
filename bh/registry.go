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
		glog.Fatalf("Error in registering hive entry: %#v", err)
	}
}

func (g *registry) unregisterHive() {
	k, _ := g.hiveRegKeyVal()
	if _, err := g.Delete(k, false); err != nil {
		glog.Fatalf("Error in unregistering hive entry: %#v", err)
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
				glog.Fatalf("Error in updating hive entry in the registry: %#v", err)
			}
			glog.V(1).Infof("Hive %s's TTL updated in registry", g.hive.ID())
		}
	}
}

func (g *registry) watchHives() {
	res, err := g.Get(g.hivePath(), false, true)
	if err != nil {
		glog.Fatalf("Cannot find the hive directory: %#v", err)
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

func (g registry) tryLockApp(id BeeID) error {
	k := g.appPath(string(id.AppName), lockFileName)

	// FIXME(soheil): This is very dangerous when the bee dies before unlock.
	_, err := g.Create(k, string(id.Bytes()), g.appTTL)
	return err
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
		return fmt.Errorf("Unlocking someone else's lock: %#v, %#v", id, v)
	}

	_, err = g.Delete(k, false)
	if err != nil {
		return err
	}

	return nil
}

func (g registry) trySyncCall(b BeeID, f func()) error {
	err := g.tryLockApp(b)
	if err != nil {
		return err
	}

	defer func() {
		err := g.unlockApp(b)
		if err != nil {
			glog.Fatalf("Cannot unlock app %#v: %#v", b, err)
		}
	}()

	f()
	return nil
}

func (g registry) syncCall(b BeeID, f func()) {
	err := g.lockApp(b)
	if err != nil {
		glog.Fatalf("Cannot lock app %#v: %#v", b, err)
	}

	defer func() {
		err := g.unlockApp(b)
		if err != nil {
			glog.Fatalf("Cannot unlock app %#v: %#v", b, err)
		}
	}()

	f()
}

func (g registry) set(col BeeColony, mc MappedCells) BeeID {
	sort.Sort(mc)

	v, err := col.Bytes()
	if err != nil {
		glog.Fatalf("Cannot serialize BeeColony: %s", err)
	}

	for _, c := range mc {
		k := g.appPath(string(col.Master.AppName), string(c.Dict), string(c.Key))
		_, err := g.Set(k, string(v), g.appTTL)
		if err != nil {
			glog.Fatalf("Cannot set bee: %+v", k)
		}
	}
	return col.Master
}

func (g registry) mappedCells(col BeeColony) (MappedCells, error) {
	p := g.appPath(string(col.Master.AppName))
	r, err := g.Get(p, false, true)
	if err != nil {
		return nil, err
	}

	var cells MappedCells
	for _, d := range r.Node.Nodes {
		for _, k := range d.Nodes {
			v, err := BeeColonyFromBytes([]byte(k.Value))
			if err != nil || !col.Equal(v) {
				continue
			}

			parts := strings.Split(k.Key, "/")
			l := len(parts)
			if l < 2 {
				continue
			}
			cells = append(cells, CellKey{
				Dict: DictName(parts[l-2]),
				Key:  Key(parts[l-1]),
			})
		}
	}
	return cells, nil
}

func (g registry) storeOrGet(col BeeColony, mc MappedCells) BeeID {
	sort.Sort(mc)

	v, err := col.Bytes()
	if err != nil {
		glog.Fatalf("Cannot serialize BeeColony: %s", err)
	}

	unsetCells := make(MappedCells, 0, len(mc))
	validate := false
	for _, c := range mc {
		k := g.appPath(string(col.Master.AppName), string(c.Dict), string(c.Key))
		res, err := g.Get(k, false, false)
		if err != nil {
			unsetCells = append(unsetCells, c)
			continue
		}

		entry, err := BeeColonyFromBytes([]byte(res.Node.Value))
		if err != nil {
			glog.Fatalf("Cannot decode a BeeColony: %s", err)
		}

		if entry.Equal(col) {
			continue
		}

		if validate {
			glog.Fatalf("Incosistencies for bee %#v: %#v != %#v", col.Master, col,
				entry)
		}

		col = entry
		v = []byte(res.Node.Value)
		validate = true
	}

	for _, c := range unsetCells {
		k := g.appPath(string(col.Master.AppName), string(c.Dict), string(c.Key))
		g.Create(k, string(v), g.appTTL)
	}

	return col.Master
}

func (g registry) compareAndSet(oldC BeeColony, newC BeeColony,
	mc MappedCells) (BeeColony, error) {
	if newC.Generation < oldC.Generation {
		return BeeColony{}, fmt.Errorf("Stale colony generation %#v < %#v",
			newC.Generation, oldC.Generation)
	}

	for _, c := range mc {
		k := g.appPath(string(oldC.Master.AppName), string(c.Dict), string(c.Key))
		res, err := g.Get(k, false, false)
		if err != nil {
			return BeeColony{}, fmt.Errorf("The mapped cells are stale: %v", err)
		}

		entry, err := BeeColonyFromBytes([]byte(res.Node.Value))
		if err != nil {
			glog.Fatalf("Cannot decode a BeeColony: %s", err)
		}

		if entry.Generation != oldC.Generation {
			return entry, fmt.Errorf("Colony generation is stale: %v != %v",
				entry.Generation, oldC.Generation)
		}
	}

	v, err := newC.Bytes()
	// TODO(soheil): We can use etcd creation and modification indices here.
	for _, c := range mc {
		k := g.appPath(string(oldC.Master.AppName), string(c.Dict), string(c.Key))
		if _, err = g.Update(k, string(v), g.appTTL); err != nil {
			glog.Fatalf("Error in updating the key: %#v", err)
		}
	}

	return newC, nil
}
