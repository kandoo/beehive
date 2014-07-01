package actor

import (
	"encoding/json"
	"fmt"

	"github.com/coreos/go-etcd/etcd"
	"github.com/golang/glog"
)

type registery struct {
	*etcd.Client
}

type regVal struct {
	StageId StageId `json:"stage_id"`
	RcvrId  uint32  `json:"rcvr_id"`
	Temp    bool    `json:"temp"`
}

func (this *regVal) Eq(that *regVal) bool {
	return this.StageId == that.StageId && this.RcvrId == that.RcvrId
}

func unmarshallRegVal(d string) (regVal, error) {
	var v regVal
	err := json.Unmarshal([]byte(d), &v)
	return v, err
}

func marshallRegVal(v regVal) (string, error) {
	b, err := json.Marshal(v)
	return string(b), err
}

const (
	keyFmtStr    = "/theatre/%s/%s/%s"
	expireAction = "expire"
)

func (g registery) lockAllOrGet(id ReceiverId, ms MapSet, ttl uint64) regVal {
	v := regVal{
		StageId: id.StageId,
		RcvrId:  id.RcvrId,
		Temp:    true,
	}

	mv, err := marshallRegVal(v)
	if err != nil {
		glog.Fatalf("Cannot marshall registery value %v: %v", v, err)
	}

	var i int
	var dk DictionaryKey
	for i, dk = range ms {
		k := fmt.Sprintf(keyFmtStr, id.ActorName, dk.Dict, dk.Key)
		for {
			res, err := g.Create(k, mv, ttl)
			if err == nil {
				goto next
			}

			res, err = g.Get(k, false, false)
			if err != nil {
				continue
			}

			tempV, err := unmarshallRegVal(res.Node.Value)
			if err != nil {
				glog.Fatalf("Cannot unmarshall registery value %v: %v", res.Node.Value,
					err)
			}

			if !tempV.Temp {
				v = tempV
				mv = res.Node.Value
				goto undo
			}

			res, err = g.Watch(k, 0, false, nil, nil)
			if err != nil {
				glog.Fatalf("Error in watching a temp lock %v: %v", k, err)
			}

			if res.Action == expireAction {
				continue
			}

			tempV, err = unmarshallRegVal(res.Node.Value)
			if err != nil {
				glog.Fatalf("Cannot unmarshall registery value %v: %v", res.Node.Value,
					err)
			}

			if tempV.Temp {
				continue
			}

			v = tempV
			mv = res.Node.Value
			goto undo
		}
	next:
	}

	v.Temp = false
	mv, err = marshallRegVal(v)
	if err != nil {
		glog.Fatalf("Cannot marshall registery value %v: %v", v, err)
	}

	for _, dk := range ms {
		k := fmt.Sprintf(keyFmtStr, id.ActorName, dk.Dict, dk.Key)
		_, err := g.Update(k, mv, ttl)
		if err != nil {
			glog.Fatalf("Cannot update the key we previously set %v: %v", k, err)
		}
	}

	return v

undo:
	for _, dk := range ms[0:i] {
		k := fmt.Sprintf(keyFmtStr, id.ActorName, dk.Dict, dk.Key)
		_, err := g.Update(k, mv, ttl)
		if err != nil {
			glog.Fatalf("Cannot update the key we previously set %v: %v", k, err)
		}
	}

	for _, dk := range ms[i:] {
		k := fmt.Sprintf(keyFmtStr, id.ActorName, dk.Dict, dk.Key)
		res, err := g.Create(k, mv, ttl)
		if err == nil {
			continue
		}

		res, err = g.Get(k, false, false)
		if err != nil {
			glog.Fatalf("Cannot create nor get the key.")
		}

		// BUG(soheil): This for sure is a bug. It's quite likely that one
		// stage fails and we get an unequal lock from a third machine.
		tempV, err := unmarshallRegVal(res.Node.Value)
		if !tempV.Eq(&v) {
			glog.Fatalf("Incosistencies in locks for key: %v")
		}
	}

	return v
}
