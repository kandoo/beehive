package beehive

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/coreos/etcd/raft/raftpb"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/golang/glog"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/gorilla/mux"
	"github.com/kandoo/beehive/Godeps/_workspace/src/golang.org/x/net/context"
	bhgob "github.com/kandoo/beehive/gob"
	"github.com/kandoo/beehive/raft"
)

// state is served as json while other endpoints serve gob. The reason is that
// state should be human readable.
const (
	serverV1StatePath   = "/api/v1/state"
	serverV1BeesPath    = "/api/v1/bees"
	serverV1MsgPath     = "/api/v1/msg"
	serverV1CmdPath     = "/api/v1/cmd"
	serverV1RaftPath    = "/api/v1/raft"
	serverV1BeeRaftPath = "/api/v1/beeraft"
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
	dec := gob.NewDecoder(r.Body)
	enc := gob.NewEncoder(w)

	for {
		var c cmd
		err := dec.Decode(&c)
		if err != nil {
			if err != io.EOF {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			return
		}

		res := h.processCommand(c)
		if res.Err != nil {
			glog.Errorf("error in running remote command: %v", res.Err)
			res.Err = bhgob.Error(res.Err.Error())
		}
		if err := enc.Encode(res); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}
}

func (h *v1Handler) processCommand(c cmd) cmdResult {
	var ctrlCh chan cmdAndChannel
	if c.App == "" {
		glog.V(3).Infof("%v handles command to hive: %v", h.srv.hive, c)
		ctrlCh = h.srv.hive.ctrlCh
	} else {
		a, ok := h.srv.hive.app(c.App)
		glog.V(3).Infof("%v handles command to app %v: %v", h.srv.hive, a, c)
		if !ok {
			return cmdResult{
				Err: bhgob.Errorf("%v cannot find app %v", h.srv.hive, c.App),
			}
		}
		if c.Bee == Nil {
			ctrlCh = a.qee.ctrlCh
		} else {
			b, ok := a.qee.beeByID(c.Bee)
			if !ok {
				return cmdResult{
					Err: bhgob.Errorf("%v cannot find bee %v", a.qee, c.Bee),
				}
			}
			ctrlCh = b.ctrlCh
		}
	}

	ch := make(chan cmdResult, 1)
	ctrlCh <- cmdAndChannel{
		cmd: c,
		ch:  ch,
	}
	for {
		select {
		case res := <-ch:
			glog.V(3).Infof("server %v returned result %#v for command %v",
				h.srv.hive.ID(), res, c)
			return res

		case <-time.After(10 * time.Second):
			glog.Errorf("%v is blocked on %v (chan size=%d)", h.srv.hive, c, len(ch))
		}
	}
}

func (h *v1Handler) handleRaft(w http.ResponseWriter, r *http.Request) {
	dec := raft.NewDecoder(r.Body)
	for {
		var msg raftpb.Message
		err := dec.Decode(&msg)
		if err != nil {
			if err != io.EOF {
				glog.Errorf("%v cannot handle raft message: %v", h.srv.hive, err)
			}
			break
		}

		if msg.To != h.srv.hive.ID() {
			glog.Errorf("%v recieves a raft message for %v", h.srv.hive, msg.To)
			continue
		}

		glog.V(3).Infof("%v handles a raft message to %v", h.srv.hive, msg.To)

		if err = h.srv.hive.stepRaft(context.TODO(), msg); err != nil {
			glog.Errorf("%v cannot step: %v", h.srv.hive, err)
		}
	}
}

func (h *v1Handler) handleBeeRaft(w http.ResponseWriter, r *http.Request) {
	dec := raft.NewDecoder(r.Body)
	var wg sync.WaitGroup
	for {
		var msg raftpb.Message
		err := dec.Decode(&msg)
		if err != nil {
			break
		}

		glog.V(3).Infof("%v handles a bee raft message for %v", h.srv.hive, msg.To)

		bi, err := h.srv.hive.bee(msg.To)
		if err != nil {
			glog.Errorf("%v cannot find bee %v", h.srv.hive, msg.To)
			continue
		}

		a, ok := h.srv.hive.app(bi.App)
		if !ok {
			glog.Errorf("%v cannot find app %v", h.srv.hive, bi.App)
			continue
		}

		b, ok := a.qee.beeByID(msg.To)
		if !ok {
			glog.Errorf("%v cannot find bee %v", h.srv.hive, msg.To)
			continue
		}

		if b.proxy || b.detached {
			glog.Errorf("%v not local to %v", b, h.srv.hive)
			continue
		}

		node := b.raftNode()
		if node == nil {
			glog.Errorf("%v's node is not started", b)
			continue
		}

		wg.Add(1)
		go func() {
			if err = b.stepRaft(msg); err != nil {
				glog.Errorf("%v cannot step: %v", b, err)
			}
			wg.Done()
		}()
	}
	wg.Wait()
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
