package composition

import (
	bh "github.com/kandoo/beehive"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/golang/glog"
)

// All composes handlers in a pipeline with the same sequence. An incoming
// message is passed to the i'th handler, if the (i-1)'th handler has
// successfully processed the incoming message.
//
// If any of these handlers returns an error, the whole transaction will be
// aborted, meaning that a message is either processed by all of these handlers
// or none of them.
func All(handlers ...bh.Handler) bh.Handler {
	if len(handlers) == 0 {
		glog.Fatalf("no handler provided")
	}

	if len(handlers) == 1 {
		return handlers[0]
	}

	return &ComposedHandler{
		Handlers: handlers,
		Composer: ComposeAll,
		Isolate:  true,
	}
}

// ComposeAll is a ComposeFunc that aborts when there is an error, and continues
// otherwise.
func ComposeAll(msg bh.Msg, ctx bh.RcvContext, err error) Step {
	if err != nil {
		return Abort
	}
	return Continue
}
