// +build go1.5

package beehive

import (
	httpPprof "net/http/pprof"

	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/gorilla/mux"
)

type pprofHandler struct{}

func (p pprofHandler) install(r *mux.Router) {
	r.HandleFunc("/debug/pprof/cmdline", httpPprof.Cmdline)
	r.HandleFunc("/debug/pprof/profile", httpPprof.Profile)
	r.HandleFunc("/debug/pprof/symbol", httpPprof.Symbol)
	r.HandleFunc("/debug/pprof/trace", httpPprof.Trace)
	r.HandleFunc("/debug/pprof/{rest:.*}", httpPprof.Index)
}
