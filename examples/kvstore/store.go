package main

import (
	"encoding/gob"
	"encoding/json"
	"errors"
	"flag"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/OneOfOne/xxhash"
	bh "github.com/kandoo/beehive"
	"github.com/kandoo/beehive/Godeps/_workspace/src/code.google.com/p/go.net/context"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/gorilla/mux"
)

const (
	dict = "store"
)

var (
	errKeyNotFound = errors.New("kvstore: key not found")
	errInternal    = errors.New("kvstore: internal error")
	errInvalid     = errors.New("kvstore: invalid parameter")
)

type put struct {
	Key string
	Val []byte
}

type get string
type result struct {
	Key string `json:"key"`
	Val string `json:"value"`
}

type del string

type kvStore struct {
	*bh.Sync
	buckets uint64
}

func (s *kvStore) Rcv(msg bh.Msg, ctx bh.RcvContext) error {
	switch data := msg.Data().(type) {
	case put:
		return ctx.Dict(dict).Put(data.Key, data.Val)
	case get:
		v, err := ctx.Dict(dict).Get(string(data))
		if err != nil {
			return errKeyNotFound
		}
		ctx.ReplyTo(msg, result{Key: string(data), Val: string(v)})
		return nil
	case del:
		return ctx.Dict(dict).Del(string(data))
	}
	return errInvalid
}

func (s *kvStore) Map(msg bh.Msg, ctx bh.MapContext) bh.MappedCells {
	var k string
	switch data := msg.Data().(type) {
	case put:
		k = string(data.Key)
	case get:
		k = string(data)
	case del:
		k = string(data)
	}
	cells := bh.MappedCells{
		{
			Dict: dict,
			Key:  strconv.FormatUint(xxhash.Checksum64([]byte(k))%s.buckets, 16),
		},
	}
	return cells
}

func (s *kvStore) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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
		res, err = s.Process(ctx, get(k))
	case "PUT":
		var v []byte
		v, err = ioutil.ReadAll(r.Body)
		if err != nil {
			break
		}
		res, err = s.Process(ctx, put{Key: k, Val: v})
	case "DELETE":
		res, err = s.Process(ctx, del(k))
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

var (
	replFactor = flag.Int("kv.rf", 3, "replication factor")
	buckets    = flag.Int("kv.b", 1024, "number of buckets")
)

func main() {
	flag.Parse()

	a := bh.NewApp("kvstore", bh.AppPersistent(*replFactor))
	//bh.AppWithPlacement(bh.RandomPlacement{Rand: rand.New(rand.NewSource(99))}))
	s := bh.NewSync(a)
	kv := &kvStore{
		Sync:    s,
		buckets: uint64(*buckets),
	}
	s.Handle(put{}, kv)
	s.Handle(get(""), kv)
	s.Handle(del(""), kv)
	a.HandleHTTP("/{key}", kv)

	bh.Start()
}

func init() {
	gob.Register(result{})
}
