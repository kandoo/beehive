// +build go1.5

package beehive

import (
	"net/http/pprof"

	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/gorilla/mux"
)

type pprofHandler struct{}

func (p pprofHandler) install(r *mux.Router) {
	r.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	r.HandleFunc("/debug/pprof/profile", pprof.Profile)
	r.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	r.HandleFunc("/debug/pprof/trace", pprof.Trace)
	r.HandleFunc("/debug/pprof/{rest:.*}", pprof.Index)
}
