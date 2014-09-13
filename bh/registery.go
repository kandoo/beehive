package bh

import (
	"fmt"
	"sort"
	"strings"
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

type registery struct {
	*etcd.Client
	hive          *hive
	prefix        string
	hiveDir       string
	hiveTTL       uint64
	appDir        string
	appTTL        uint64
	watchCancelCh chan bool
	watchJoinCh   chan bool
	ttlCancelCh   chan chan bool
}

func (h *hive) connectToRegistery() {
	if len(h.config.RegAddrs) == 0 {
		return
	}

	// TODO(soheil): Add TLS registery.
	h.registery = registery{
		Client:  etcd.NewClient(h.config.RegAddrs),
		hive:    h,
		prefix:  regPrefix,
		hiveDir: regHiveDir,
		hiveTTL: regHiveTTL,
		appDir:  regAppDir,
		appTTL:  regAppTTL,
	}

	if ok := h.registery.SyncCluster(); !ok {
		glog.Fatalf("Cannot connect to registery nodes: %s", h.config.RegAddrs)
	}

	h.RegisterMsg(HiveJoined{})
	h.RegisterMsg(HiveLeft{})
	h.registery.registerHive()
	h.registery.startPollers()
}

func (g *registery) disconnect() {
	if !g.connected() {
		return
	}

	g.watchCancelCh <- true
	<-g.watchJoinCh

	cancelRes := make(chan bool)
	g.ttlCancelCh <- cancelRes
	<-cancelRes

	g.unregisterHive()
}

func (g registery) connected() bool {
	return g.Client != nil
}

func (g *registery) hiveRegKeyVal() (string, string) {
	v := string(g.hive.ID())
	return g.hivePath(v), v
}

func (g *registery) registerHive() {
	k, v := g.hiveRegKeyVal()
	if _, err := g.Create(k, v, g.hiveTTL); err != nil {
		glog.Fatalf("Error in registering hive entry: %v", err)
	}
}

func (g *registery) unregisterHive() {
	k, _ := g.hiveRegKeyVal()
	if _, err := g.Delete(k, false); err != nil {
		glog.Fatalf("Error in unregistering hive entry: %v", err)
	}
}

func (g *registery) startPollers() {
	g.ttlCancelCh = make(chan chan bool)
	go g.updateTTL()

	g.watchCancelCh = make(chan bool)
	g.watchJoinCh = make(chan bool)
	go g.watchHives()
}

func (g *registery) updateTTL() {
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
				glog.Fatalf("Error in updating hive entry in the registery: %v", err)
			}
			glog.V(1).Infof("Hive %s's TTL updated in registery", g.hive.ID())
		}
	}
}

func (g *registery) watchHives() {
	res, err := g.Get(g.hivePath(), false, true)
	if err != nil {
		glog.Fatalf("Cannot find the hive directory: %v", err)
	}

	for _, n := range res.Node.Nodes {
		g.hive.Emit(HiveJoined{g.hiveIDFromPath(n.Key)})
	}

	resCh := make(chan *etcd.Response)
	joinCh := make(chan bool)
	go func() {
		g.Watch(g.hivePath(), 0, true, resCh, g.watchCancelCh)
		joinCh <- true
	}()

	for {
		select {
		case <-joinCh:
			g.watchJoinCh <- true
			return
		case res := <-resCh:
			if res == nil {
				continue
			}

			switch res.Action {
			case "create":
				if res.PrevNode == nil {
					g.hive.Emit(HiveJoined{g.hiveIDFromPath(res.Node.Key)})
				}
			case "delete":
				if res.PrevNode != nil {
					g.hive.Emit(HiveLeft{g.hiveIDFromPath(res.Node.Key)})
				}
			default:
				glog.V(2).Infof("Received an update from registery: %+v", *res)
			}
		}
	}
}

func (g registery) path(elem ...string) string {
	return g.prefix + "/" + strings.Join(elem, "/")
}

func (g registery) appPath(elem ...string) string {
	return g.prefix + "/" + g.appDir + "/" + strings.Join(elem, "/")
}

func (g registery) hivePath(elem ...string) string {
	return g.prefix + "/" + g.hiveDir + "/" + strings.Join(elem, "/")
}

func (g registery) hiveIDFromPath(path string) HiveID {
	prefixLen := len(g.hivePath()) + 1
	return HiveID(path[prefixLen:])
}

func (g registery) lockApp(id BeeID) error {
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

func (g registery) unlockApp(id BeeID) error {
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

func (g registery) set(id BeeID, ms MappedCells) BeeID {
	err := g.lockApp(id)
	if err != nil {
		glog.Fatalf("Cannot lock app %v: %v", id, err)
	}

	defer func() {
		err := g.unlockApp(id)
		if err != nil {
			glog.Fatalf("Cannot unlock app %v: %v", id, err)
		}
	}()

	sort.Sort(ms)

	v := string(id.Bytes())
	for _, dk := range ms {
		k := g.appPath(string(id.AppName), string(dk.Dict), string(dk.Key))
		_, err := g.Set(k, v, g.appTTL)
		if err != nil {
			glog.Fatalf("Cannot set bee: %+v", k)
		}
	}
	return id
}

func (g registery) storeOrGet(id BeeID, ms MappedCells) BeeID {
	err := g.lockApp(id)
	if err != nil {
		glog.Fatalf("Cannot lock app %v: %v", id, err)
	}

	defer func() {
		err := g.unlockApp(id)
		if err != nil {
			glog.Fatalf("Cannot unlock app %v: %v", id, err)
		}
	}()

	sort.Sort(ms)

	v := string(id.Bytes())
	validate := false
	for _, dk := range ms {
		k := g.appPath(string(id.AppName), string(dk.Dict), string(dk.Key))
		res, err := g.Get(k, false, false)
		if err != nil {
			continue
		}

		entryID := BeeIDFromBytes([]byte(res.Node.Value))
		if entryID == id {
			continue
		}

		if validate {
			glog.Fatalf("Incosistencies for bee %v: %v", id, entryID)
		}

		id = entryID
		v = res.Node.Value
		validate = true
	}

	for _, dk := range ms {
		k := g.appPath(string(id.AppName), string(dk.Dict), string(dk.Key))
		g.Create(k, v, g.appTTL)
	}

	return id
}
