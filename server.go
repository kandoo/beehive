package beehive

import (
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/soheilhy/beehive/Godeps/_workspace/src/code.google.com/p/go.net/context"
	"github.com/soheilhy/beehive/Godeps/_workspace/src/github.com/coreos/etcd/raft/raftpb"
	"github.com/soheilhy/beehive/Godeps/_workspace/src/github.com/golang/glog"
	"github.com/soheilhy/beehive/Godeps/_workspace/src/github.com/gorilla/mux"
	bhgob "github.com/soheilhy/beehive/gob"
)

const (
	serverV1MsgPath    = "/hive/v1/msg"
	serverV1MsgFormat  = "http://%s" + serverV1MsgPath
	serverV1CmdPath    = "/hive/v1/cmd/"
	serverV1CmdFormat  = "http://%s" + serverV1CmdPath
	serverV1RaftPath   = "/hive/v1/raft/"
	serverV1RaftFormat = "http://%s" + serverV1RaftPath
)

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
		glog.V(2).Infof("Server %v handles command to hive: %v", h.srv.hive.ID(), c)
		ctrlCh = h.srv.hive.ctrlCh
	} else {
		a, ok := h.srv.hive.app(c.App)
		glog.V(2).Infof("Server %v handles command to app %v: %v", h.srv.hive.ID(),
			a, c)
		if !ok {
			http.Error(w, fmt.Sprintf("Cannot find app %s", c.App),
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
				res.Err = bhgob.GobError{res.Err.Error()}
			}

			if err := gob.NewEncoder(w).Encode(res); err != nil {
				glog.Errorf("Error in encoding the command results: %s", err)
			}

			return
		case <-time.After(1 * time.Second):
			glog.Fatalf("Server is blocked on %v", c)
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
