package beehive

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/kandoo/beehive/Godeps/_workspace/src/code.google.com/p/go.net/context"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/coreos/etcd/raft/raftpb"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/golang/glog"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/gorilla/mux"
	bhgob "github.com/kandoo/beehive/gob"
)

// state is served as json while other endpoints serve gob. The reason is that
// state should be human readable.
const (
	serverV1StatePath   = "/api/v1/state"
	serverV1BeesPath    = "/api/v1/bees"
	serverV1MsgPath     = "/api/v1/msg"
	serverV1CmdPath     = "/api/v1/cmd"
	serverV1RaftPath    = "/api/v1/raft"
	serverV1BeeRaftPath = serverV1RaftPath + "/{app}/{id}"
)

func buildURL(scheme, addr, path string) string {
	var buffer bytes.Buffer
	buffer.WriteString(scheme)
	buffer.WriteString("://")
	buffer.WriteString(addr)
	buffer.WriteString(path)
	return buffer.String()
}

// server is the HTTP server that act as the remote endpoint for Beehive.
type server struct {
	http.Server

	hive   *hive
	router *mux.Router
}

// newServer creates a new server for the hive.
func newServer(h *hive, addr string) *server {
	r := mux.NewRouter()
	s := &server{
		Server: http.Server{
			Addr:    addr,
			Handler: r,
		},
		router: r,
		hive:   h,
	}
	handlerV1 := v1Handler{srv: s}
	handlerV1.install(r)
	webHandler := webHandler{h: h}
	webHandler.install(r)
	return s
}

// Provides the net/http interface for the server.
func (s *server) HandleFunc(p string,
	h func(http.ResponseWriter, *http.Request)) {

	s.router.HandleFunc(p, h)
}

type v1Handler struct {
	srv *server
}

func (h *v1Handler) install(r *mux.Router) {
	r.HandleFunc(serverV1StatePath, h.handleHiveState)
	r.HandleFunc(serverV1BeesPath, h.handleBees)
	r.HandleFunc(serverV1MsgPath, h.handleMsg)
	r.HandleFunc(serverV1CmdPath, h.handleCmd)
	r.HandleFunc(serverV1BeeRaftPath, h.handleBeeRaft)
	r.HandleFunc(serverV1RaftPath, h.handleRaft)
}

func (h *v1Handler) handleMsg(w http.ResponseWriter, r *http.Request) {
	dec := gob.NewDecoder(r.Body)
	var err error
	for {
		var m msg
		err = dec.Decode(&m)
		if err != nil {
			break
		}
		h.srv.hive.enqueMsg(&m)
	}
	if err != io.EOF {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
}

func (h *v1Handler) handleCmd(w http.ResponseWriter, r *http.Request) {
	var c cmd
	if err := gob.NewDecoder(r.Body).Decode(&c); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ch := make(chan cmdResult)
	var ctrlCh chan cmdAndChannel
	if c.App == "" {
		glog.V(2).Infof("server %v handles command to hive: %v", h.srv.hive.ID(), c)
		ctrlCh = h.srv.hive.ctrlCh
	} else {
		a, ok := h.srv.hive.app(c.App)
		glog.V(2).Infof("server %v handles command to app %v: %v", h.srv.hive.ID(),
			a, c)
		if !ok {
			http.Error(w, fmt.Sprintf("cannot find app %s", c.App),
				http.StatusBadRequest)
			return
		}
		ctrlCh = a.qee.ctrlCh
	}

	ctrlCh <- cmdAndChannel{
		cmd: c,
		ch:  ch,
	}
	for {
		select {
		case res := <-ch:
			if res.Err != nil {
				glog.Errorf("Error in running the remote command: %v", res.Err)
				res.Err = bhgob.Error(res.Err.Error())
			}

			if err := gob.NewEncoder(w).Encode(res); err != nil {
				glog.Errorf("Error in encoding the command results: %s", err)
				return
			}

			glog.V(2).Infof("server %v returned result %#v for command %v",
				h.srv.hive.ID(), res, c)
			return
		case <-time.After(1 * time.Second):
			glog.Errorf("Server is blocked on %v", c)
		}
	}
}

func (h *v1Handler) handleRaft(w http.ResponseWriter, r *http.Request) {
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var msg raftpb.Message
	if err = msg.Unmarshal(b); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err = h.srv.hive.stepRaft(context.TODO(), msg); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (h *v1Handler) handleBeeRaft(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	a, ok := h.srv.hive.app(vars["app"])
	if !ok {
		http.Error(w, fmt.Sprintf("cannot find app %s", a.Name()),
			http.StatusBadRequest)
		return
	}

	id, err := strconv.ParseUint(vars["id"], 10, 64)
	if err != nil {
		glog.Errorf("cannot parse id %v: %v", vars["id"], err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	b, ok := a.qee.beeByID(id)
	if !ok {
		glog.Errorf("%v cannot find bee %v", h.srv.hive, id)
		http.Error(w, fmt.Sprintf("cannot find bee %v", id), http.StatusBadRequest)
		return
	}

	if b.proxy || b.detached {
		glog.Errorf("%v not local to %v", b, h.srv.hive)
		http.Error(w, fmt.Sprintf("not a local bee %v", id), http.StatusBadRequest)
		return
	}

	buf, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var msg raftpb.Message
	if err = msg.Unmarshal(buf); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	node := b.raftNode()
	if node == nil {
		http.Error(w, "node is not started", http.StatusInternalServerError)
		return
	}
	if err = b.stepRaft(msg); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

type hiveState struct {
	Id    uint64     `json:"id"`
	Addr  string     `json:"addr"`
	Peers []HiveInfo `json:"peers"`
}

func (h *v1Handler) handleHiveState(w http.ResponseWriter, r *http.Request) {
	s := hiveState{
		Id:    h.srv.hive.ID(),
		Addr:  h.srv.hive.config.Addr,
		Peers: h.srv.hive.registry.hives(),
	}

	j, err := json.Marshal(s)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(j)
}

func (h *v1Handler) handleBees(w http.ResponseWriter, r *http.Request) {
	bees := h.srv.hive.registry.bees()
	j, err := json.Marshal(bees)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(j)
}
