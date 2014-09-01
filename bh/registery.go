package bh

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/coreos/go-etcd/etcd"
	"github.com/golang/glog"
)

// Emitted when a hive joins the cluster. Note that this message is emitted on
// all hives.
type HiveJoined struct {
	HiveId HiveId // The ID of the hive.
}

// Emitted when a hive leaves the cluster. Note that this event is emitted on
// all hives.
type HiveLeft struct {
	HiveId HiveId // The ID of the hive.
}

const (
	regPrefix    = "beehive"
	regAppDir    = "apps"
	regHiveDir   = "hives"
	regAppTtl    = 0
	regHiveTtl   = 60
	expireAction = "expire"
	lockFileName = "__lock__"
)

type registery struct {
	*etcd.Client
	hive          *hive
	prefix        string
	hiveDir       string
	hiveTtl       uint64
	appDir        string
	appTtl        uint64
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
		hiveTtl: regHiveTtl,
		appDir:  regAppDir,
		appTtl:  regAppTtl,
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
	v := string(g.hive.Id())
	return g.hivePath(v), v
}

func (g *registery) registerHive() {
	k, v := g.hiveRegKeyVal()
	if _, err := g.Create(k, v, g.hiveTtl); err != nil {
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
	go g.updateTtl()

	g.watchCancelCh = make(chan bool)
	g.watchJoinCh = make(chan bool)
	go g.watchHives()
}

func (g *registery) updateTtl() {
	waitTimeout := g.hiveTtl / 2
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
			if _, err := g.Update(k, v, g.hiveTtl); err != nil {
				glog.Fatalf("Error in updating hive entry in the registery: %v", err)
			}
			glog.V(1).Infof("Hive %s's TTL updated in registery", g.hive.Id())
		}
	}
}

func (g *registery) watchHives() {
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
					g.hive.Emit(HiveJoined{g.hiveIdFromPath(res.Node.Key)})
				}
			case "delete":
				if res.PrevNode != nil {
					g.hive.Emit(HiveLeft{g.hiveIdFromPath(res.Node.Key)})
				}
			default:
				glog.V(2).Infof("Received an update from registery: %+v", *res)
			}
		}
	}
}

type beeRegVal struct {
	HiveId HiveId `json:"hive_id"`
	BeeId  uint32 `json:"bee_id"`
}

func (this *beeRegVal) Eq(that *beeRegVal) bool {
	return this.HiveId == that.HiveId && this.BeeId == that.BeeId
}

func unmarshallRegVal(d string) (beeRegVal, error) {
	var v beeRegVal
	err := json.Unmarshal([]byte(d), &v)
	return v, err
}

func unmarshallRegValOrFail(d string) beeRegVal {
	v, err := unmarshallRegVal(d)
	if err != nil {
		glog.Fatalf("Cannot unmarshall registery value %v: %v", d, err)
	}
	return v
}

func marshallRegVal(v beeRegVal) (string, error) {
	b, err := json.Marshal(v)
	return string(b), err
}

func marshallRegValOrFail(v beeRegVal) string {
	d, err := marshallRegVal(v)
	if err != nil {
		glog.Fatalf("Cannot marshall registery value %v: %v", v, err)
	}
	return d
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

func (g registery) hiveIdFromPath(path string) HiveId {
	prefixLen := len(g.hivePath()) + 1
	return HiveId(path[prefixLen:])
}

func (g registery) lockApp(id BeeId) error {
	// TODO(soheil): For lock and unlock we can use etcd indices but
	// v.Temp might be changed by the app. Check this and fix it if possible.
	v := beeRegVal{
		HiveId: id.HiveId,
		BeeId:  id.Id,
	}
	k := g.appPath(string(id.AppName), lockFileName)

	for {
		_, err := g.Create(k, marshallRegValOrFail(v), g.appTtl)
		if err == nil {
			return nil
		}

		_, err = g.Watch(k, 0, false, nil, nil)
		if err != nil {
			return err
		}
	}
}

func (g registery) unlockApp(id BeeId) error {
	v := beeRegVal{
		HiveId: id.HiveId,
		BeeId:  id.Id,
	}
	k := g.appPath(string(id.AppName), lockFileName)

	res, err := g.Get(k, false, false)
	if err != nil {
		return err
	}

	tempV := unmarshallRegValOrFail(res.Node.Value)
	if !v.Eq(&tempV) {
		return errors.New(
			fmt.Sprintf("Unlocking someone else's lock: %v, %v", v, tempV))
	}

	_, err = g.Delete(k, false)
	if err != nil {
		return err
	}

	return nil
}

func (g registery) set(id BeeId, ms MapSet) beeRegVal {
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

	v := beeRegVal{
		HiveId: id.HiveId,
		BeeId:  id.Id,
	}
	mv := marshallRegValOrFail(v)
	for _, dk := range ms {
		k := g.appPath(string(id.AppName), string(dk.Dict), string(dk.Key))
		_, err := g.Set(k, mv, g.appTtl)
		if err != nil {
			glog.Fatalf("Cannot set bee: %+v", k)
		}
	}
	return v
}

func (g registery) storeOrGet(id BeeId, ms MapSet) beeRegVal {
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

	v := beeRegVal{
		HiveId: id.HiveId,
		BeeId:  id.Id,
	}
	mv := marshallRegValOrFail(v)
	validate := false
	for _, dk := range ms {
		k := g.appPath(string(id.AppName), string(dk.Dict), string(dk.Key))
		res, err := g.Get(k, false, false)
		if err != nil {
			continue
		}

		resV := unmarshallRegValOrFail(res.Node.Value)
		if resV.Eq(&v) {
			continue
		}

		if validate {
			glog.Fatalf("Incosistencies for bee %v: %v, %v", id, v, resV)
		}

		v = resV
		mv = res.Node.Value
		validate = true
	}

	for _, dk := range ms {
		k := g.appPath(string(id.AppName), string(dk.Dict), string(dk.Key))
		g.Create(k, mv, g.appTtl)
	}

	return v
}
