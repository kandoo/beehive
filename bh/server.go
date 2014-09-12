package bh

import (
	"encoding/gob"
	"fmt"
	"net"
	"net/http"

	"github.com/golang/glog"
	"github.com/gorilla/mux"
)

type handShakeType int

func (h *hive) listen() error {
	l, e := net.Listen("tcp", h.config.HiveAddr)
	if e != nil {
		glog.Errorf("Hive cannot listen: %v", e)
		return e
	}
	glog.Infof("Hive listens at: %s", h.config.HiveAddr)
	h.listener = l

	s := h.newServer(h.config.HiveAddr)
	go s.Serve(l)
	return nil
}

// Server is the HTTP server that act as the remote endpoint for Beehive.
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

// NewServer creates a server for the given addr. It installs all required
// handlers for Beehive.
func (h *hive) newServer(addr string) *server {
	r := mux.NewRouter()
	s := server{
		Server: http.Server{
			Addr:    addr,
			Handler: r,
		},
		router: r,
		hive:   h,
	}

	handlerV1 := v1Handler{
		srv: &s,
	}
	handlerV1.Install(r)

	return &s
}

type v1Handler struct {
	srv *server
}

func (h *v1Handler) Install(r *mux.Router) {
	r.HandleFunc("/hive/v1/msg", h.handleMsg)
	r.HandleFunc("/hive/v1/cmd", h.handleCmd)
}

func (h *v1Handler) handleMsg(w http.ResponseWriter, r *http.Request) {
	var msg msg
	if err := gob.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	a, ok := h.srv.hive.app(msg.MsgTo.AppName)
	if !ok {
		http.Error(w, fmt.Sprintf("Cannot find app %s", msg.MsgTo.AppName),
			http.StatusBadRequest)
		return
	}

	a.qee.dataCh <- msgAndHandler{&msg, a.handlers[msg.Type()]}
}

func (h *v1Handler) handleCmd(w http.ResponseWriter, r *http.Request) {
	var cmd RemoteCmd
	if err := gob.NewDecoder(r.Body).Decode(&cmd); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resCh := make(chan CmdResult)

	a, ok := h.srv.hive.app(cmd.CmdTo.AppName)
	if !ok {
		http.Error(w, fmt.Sprintf("Cannot find app %s", cmd.CmdTo.AppName),
			http.StatusBadRequest)
		return
	}

	a.qee.ctrlCh <- LocalCmd{
		CmdType: cmd.CmdType,
		CmdData: cmd.CmdData,
		ResCh:   resCh,
	}

	res := <-resCh

	if res.Err != nil {
		glog.Errorf("Error in running the remote command: %v", res.Err)
	}

	if err := gob.NewEncoder(w).Encode(res); err != nil {
		glog.Errorf("Error in encoding the command results: %s", err)
		return
	}
}
