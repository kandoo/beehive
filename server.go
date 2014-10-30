package beehive

import (
	"bytes"
	"encoding/gob"
	"fmt"
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

const (
	serverV1StatusPath  = "/api/v1/status"
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

// Provides the net/http interface for the server.
func (s *server) HandleFunc(p string,
	h func(http.ResponseWriter, *http.Request)) {

	s.router.HandleFunc(p, h)
}

type v1Handler struct {
	srv *server
}

func (h *v1Handler) Install(r *mux.Router) {
	r.HandleFunc(serverV1MsgPath, h.handleMsg)
	r.HandleFunc(serverV1CmdPath, h.handleCmd)
	r.HandleFunc(serverV1BeeRaftPath, h.handleBeeRaft)
	r.HandleFunc(serverV1RaftPath, h.handleRaft)
}

func (h *v1Handler) handleMsg(w http.ResponseWriter, r *http.Request) {
	var msg msg
	if err := gob.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	h.srv.hive.dataCh <- &msg
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
				res.Err = bhgob.GobError{Err: res.Err.Error()}
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

	if err = h.srv.hive.processRaft(context.TODO(), msg); err != nil {
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

	lb, ok := b.(*localBee)
	if !ok {
		glog.Errorf("%v is not local to %v", b, h.srv.hive)
		http.Error(w, fmt.Sprintf("not local bee %v", id), http.StatusBadRequest)
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

	if err = lb.processRaft(msg); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
