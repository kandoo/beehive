package store

import (
	"encoding/gob"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	bh "github.com/kandoo/beehive"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/OneOfOne/xxhash"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/gorilla/mux"
	"github.com/kandoo/beehive/Godeps/_workspace/src/golang.org/x/net/context"
)

const (
	dict = "S"
)

var (
	errKeyNotFound = errors.New("kvstore: key not found")
	errInternal    = errors.New("kvstore: internal error")
	errInvalid     = errors.New("kvstore: invalid parameter")
)

type Put struct {
	Key string
	Val string
}

type Get string
type Result struct {
	Key string `json:"key"`
	Val string `json:"value"`
}

type Del string

type KVStore struct {
	Hive    bh.Hive
	Buckets uint64
}

func (s *KVStore) Rcv(msg bh.Msg, ctx bh.RcvContext) error {
	switch data := msg.Data().(type) {
	case Put:
		return ctx.Dict(dict).Put(data.Key, data.Val)
	case Get:
		v, err := ctx.Dict(dict).Get(string(data))
		if err != nil {
			return errKeyNotFound
		}
		ctx.Reply(msg, Result{Key: string(data), Val: v.(string)})
		return nil
	case Del:
		return ctx.Dict(dict).Del(string(data))
	}
	return errInvalid
}

func (s *KVStore) Map(msg bh.Msg, ctx bh.MapContext) bh.MappedCells {
	var k string
	switch data := msg.Data().(type) {
	case Put:
		k = string(data.Key)
	case Get:
		k = string(data)
	case Del:
		k = string(data)
	}
	cells := bh.MappedCells{
		{
			Dict: dict,
			Key:  strconv.FormatUint(xxhash.Checksum64([]byte(k))%s.Buckets, 16),
		},
	}
	return cells
}

func (s *KVStore) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	k, ok := mux.Vars(r)["key"]
	if !ok {
		http.Error(w, "no key in the url", http.StatusBadRequest)
		return
	}

	ctx, cnl := context.WithTimeout(context.Background(), 30*time.Second)
	var res interface{}
	var err error
	switch r.Method {
	case "GET":
		res, err = s.Hive.Sync(ctx, Get(k))
	case "PUT":
		var v []byte
		v, err = ioutil.ReadAll(r.Body)
		if err != nil {
			break
		}
		res, err = s.Hive.Sync(ctx, Put{Key: k, Val: string(v)})
	case "DELETE":
		res, err = s.Hive.Sync(ctx, Del(k))
	}
	cnl()

	if err != nil {
		switch {
		case err.Error() == errKeyNotFound.Error():
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		case err.Error() == errInternal.Error():
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		case err.Error() == errInvalid.Error():
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	if res == nil {
		return
	}

	js, err := json.Marshal(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(js)
}

func init() {
	gob.Register(Result{})
}
