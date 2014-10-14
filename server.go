package beehive

import (
	"encoding/gob"
	"fmt"
	"net/http"
	"time"

	"github.com/golang/glog"
	"github.com/gorilla/mux"
	bhgob "github.com/soheilhy/beehive/gob"
)

const (
	serverV1MsgPath   = "/hive/v1/msg"
	serverV1MsgFormat = "http://%s" + serverV1MsgPath
	serverV1CmdPath   = "/hive/v1/cmd/"
	serverV1CmdFormat = "http://%s" + serverV1CmdPath
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

	glog.V(2).Infof("Server %v handles command %v", h.srv.hive.ID(), c)

	a, ok := h.srv.hive.app(c.App)
	if !ok {
		http.Error(w, fmt.Sprintf("Cannot find app %s", c.App),
			http.StatusBadRequest)
		return
	}

	ch := make(chan cmdResult)
	a.qee.ctrlCh <- newCmdAndChannel(c.Data, c.App, c.To, ch)
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
