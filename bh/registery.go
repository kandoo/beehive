package bh

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/coreos/go-etcd/etcd"
	"github.com/golang/glog"
)

const (
	regPrefix = "theatre"
	regTtl    = 0
)

type registery struct {
	*etcd.Client
	prefix string
	ttl    uint64
}

func (s *stage) connectToRegistery() {
	if len(s.config.RegAddrs) == 0 {
		return
	}

	// TODO(soheil): Add TLS registery.
	s.registery = registery{etcd.NewClient(s.config.RegAddrs), regPrefix, regTtl}
	if ok := s.registery.SyncCluster(); !ok {
		glog.Fatalf("Cannot connect to registery nodes: %s", s.config.RegAddrs)
	}
}

func (g registery) connected() bool {
	return g.Client == nil
}

type regVal struct {
	StageId StageId `json:"stage_id"`
	RcvrId  uint32  `json:"rcvr_id"`
}

func (this *regVal) Eq(that *regVal) bool {
	return this.StageId == that.StageId && this.RcvrId == that.RcvrId
}

func unmarshallRegVal(d string) (regVal, error) {
	var v regVal
	err := json.Unmarshal([]byte(d), &v)
	return v, err
}

func unmarshallRegValOrFail(d string) regVal {
	v, err := unmarshallRegVal(d)
	if err != nil {
		glog.Fatalf("Cannot unmarshall registery value %v: %v", d, err)
	}
	return v
}

func marshallRegVal(v regVal) (string, error) {
	b, err := json.Marshal(v)
	return string(b), err
}

func marshallRegValOrFail(v regVal) string {
	d, err := marshallRegVal(v)
	if err != nil {
		glog.Fatalf("Cannot marshall registery value %v: %v", v, err)
	}
	return d
}

const (
	keyFmtStr    = "/theatre/%s/%s/%s"
	expireAction = "expire"
	lockFileName = "__lock__"
)

func (g registery) path(elem ...string) string {
	return g.prefix + "/" + strings.Join(elem, "/")
}

func (g registery) lockApp(id RcvrId) error {
	// TODO(soheil): For lock and unlock we can use etcd indices but
	// v.Temp might be changed by the app. Check this and fix it if possible.
	v := regVal{
		StageId: id.StageId,
		RcvrId:  id.Id,
	}
	k := g.path(string(id.AppName), lockFileName)

	for {
		_, err := g.Create(k, marshallRegValOrFail(v), g.ttl)
		if err == nil {
			return nil
		}

		_, err = g.Watch(k, 0, false, nil, nil)
		if err != nil {
			return err
		}
	}
}

func (g registery) unlockApp(id RcvrId) error {
	v := regVal{
		StageId: id.StageId,
		RcvrId:  id.Id,
	}
	k := g.path(string(id.AppName), lockFileName)

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

func (g registery) set(id RcvrId, ms MapSet) regVal {
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

	v := regVal{
		StageId: id.StageId,
		RcvrId:  id.Id,
	}
	mv := marshallRegValOrFail(v)
	for _, dk := range ms {
		k := fmt.Sprintf(keyFmtStr, id.AppName, dk.Dict, dk.Key)
		_, err := g.Set(k, mv, g.ttl)
		if err != nil {
			glog.Fatalf("Cannot set receiver: %+v", k)
		}
	}
	return v
}

func (g registery) storeOrGet(id RcvrId, ms MapSet) regVal {
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

	v := regVal{
		StageId: id.StageId,
		RcvrId:  id.Id,
	}
	mv := marshallRegValOrFail(v)
	validate := false
	for _, dk := range ms {
		k := fmt.Sprintf(keyFmtStr, id.AppName, dk.Dict, dk.Key)
		res, err := g.Get(k, false, false)
		if err != nil {
			continue
		}

		resV := unmarshallRegValOrFail(res.Node.Value)
		if resV.Eq(&v) {
			continue
		}

		if validate {
			glog.Fatalf("Incosistencies for receiver %v: %v, %v", id, v, resV)
		}

		v = resV
		mv = res.Node.Value
		validate = true
	}

	for _, dk := range ms {
		k := fmt.Sprintf(keyFmtStr, id.AppName, dk.Dict, dk.Key)
		g.Create(k, mv, g.ttl)
	}

	return v
}
