package composition

import (
	bh "github.com/kandoo/beehive"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/golang/glog"
)

// Any composes handlers as a logic OR function. An incoming message is passed
// to the i'th handler, if the (i-1)'th handler cannot successfully process the
// incoming message.
func Any(handlers ...bh.Handler) bh.Handler {
	if len(handlers) == 0 {
		glog.Fatalf("no handler provided")
	}

	if len(handlers) == 1 {
		return handlers[0]
	}

	return &ComposedHandler{
		Handlers: handlers,
		Composer: ComposeAny,
		Isolate:  true,
	}
}

// ComposeAny is a ComposeFunc that Commit's when is no error, and
// ContinueOrAbort's if there is an error.
func ComposeAny(msg bh.Msg, ctx bh.RcvContext, err error) Step {
	if err == nil {
		return Commit
	}
	return ContinueOrAbort
}
