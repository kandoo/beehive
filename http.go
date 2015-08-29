package beehive

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"net/http"

	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/gorilla/mux"
)

// state is served as json while other endpoints serve gob. The reason is that
// state should be human readable.
const (
	serverV1StatePath = "/api/v1/state"
	serverV1BeesPath  = "/api/v1/bees"
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
type httpServer struct {
	http.Server

	hive   *hive
	router *mux.Router
}

// newServer creates a new server for the hive.
func newServer(h *hive) *httpServer {
	r := mux.NewRouter()
	s := &httpServer{
		Server: http.Server{
			Addr:    h.config.Addr,
			Handler: r,
		},
		router: r,
		hive:   h,
	}
	v1 := v1Handler{srv: s}
	v1.install(r)
	w := webHandler{}
	w.install(r)
	if h.config.Pprof {
		p := pprofHandler{}
		p.install(r)
	}
	return s
}

// Provides the net/http interface for the server.
func (s *httpServer) HandleFunc(p string,
	h func(http.ResponseWriter, *http.Request)) {

	s.router.HandleFunc(p, h)
}

type v1Handler struct {
	srv *httpServer
}

func (h *v1Handler) install(r *mux.Router) {
	r.HandleFunc(serverV1StatePath, h.handleHiveState)
	r.HandleFunc(serverV1BeesPath, h.handleBees)
}

func (h *v1Handler) handleHiveState(w http.ResponseWriter, r *http.Request) {
	s := HiveState{
		ID:    h.srv.hive.ID(),
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

func init() {
	gob.Register(HiveState{})
}
