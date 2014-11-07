package composition

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	bh "github.com/kandoo/beehive"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/golang/glog"
)

// ComposedHandler composes a set of handlers (ie, Handlers) using the Composer.
type ComposedHandler struct {
	Handlers []bh.Handler // handlers to be composed.
	Composer ComposeFunc  // the Composer.
	Isolate  bool         // whether to isolate the dictionaries.
}

func (c *ComposedHandler) callRcv(h bh.Handler, msg bh.Msg, ctx bh.RcvContext) (
	err error) {

	defer func() {
		r := recover()
		if r == nil {
			return
		}

		if d, ok := r.(time.Duration); ok {
			ctx.Snooze(d)
		}

		err = errors.New(fmt.Sprintf("%v", r))
	}()

	return h.Rcv(msg, ctx)
}

func (c *ComposedHandler) callMap(h bh.Handler, msg bh.Msg, ctx bh.MapContext) (
	cells bh.MappedCells, err error) {

	defer func() {
		if r := recover(); r != nil {
			err = errors.New(fmt.Sprintf("%v", r))
		}
	}()

	cells = h.Map(msg, ctx)
	if cells == nil {
		err = fmt.Errorf("%#v drops the msg", h)
	}
	return
}

// Rcv method of the composed handler.
func (c *ComposedHandler) Rcv(msg bh.Msg, ctx bh.RcvContext) error {
	var err error
	for i := range c.Handlers {
		if c.Isolate {
			rctx := composedRcvContext{RcvContext: ctx, prefix: strconv.Itoa(i)}
			err = c.callRcv(c.Handlers[i], msg, rctx)
		} else {
			err = c.callRcv(c.Handlers[i], msg, ctx)
		}

		switch c.Composer(msg, ctx, err) {
		case Abort:
			ctx.AbortTx()
			return nil
		case Commit:
			ctx.CommitTx()
			return nil
		case Continue:
			if i == len(c.Handlers)-1 {
				if err == nil {
					ctx.CommitTx()
				} else {
					ctx.AbortTx()
				}
				return nil
			}
		case ContinueOrAbort:
			if i == len(c.Handlers)-1 {
				ctx.AbortTx()
				return nil
			}
		}
	}
	return nil
}

// Map method of the composed handler.
func (c *ComposedHandler) Map(msg bh.Msg, ctx bh.MapContext) bh.MappedCells {
	var cells bh.MappedCells
	var err error
	for i, h := range c.Handlers {
		var hc bh.MappedCells
		if c.Isolate {
			mctx := composedMapContext{MapContext: ctx, prefix: strconv.Itoa(i)}
			hc, err = c.callMap(h, msg, mctx)
		} else {
			hc, err = c.callMap(h, msg, ctx)
		}

		// TODO(soheil): Is there any better way to handle this?
		if err != nil {
			glog.Errorf("error in calling the map function of %#v: %v", h, err)
			return nil
		}
		if c.Isolate {
			for j := range hc {
				hc[j].Dict = strconv.Itoa(i) + hc[j].Dict
			}
		}
		for _, cell := range hc {
			cells = append(cells, cell)
		}
	}
	return cells
}

type Step int

const (
	// Continue means that the composer should continue to the next handler,
	// leaving the transcation open. If all handlers are already called, continue
	// results either in a commit if there no error, or otherwise in an abort.
	Continue Step = iota
	// Abort means that the composer should immidiately abort the transaction and
	// return.
	Abort
	// Commit means that the composer should immmidiately commit the transcation
	// and return.
	Commit
	// ContinueOrAbort acts similar to Coninue except, when there is no handlers
	// left, it will result in an abort.
	ContinueOrAbort
)

// ComposeFunc implements a single step for a ComposedHandler. At step i, the
// ComposedHandler calls handler i, and then invokes this function with the msg,
// the receiver context and the error of calling the handler (if any). The
// ComposeFunc in response tells the ComposedHandler how to proceed.
type ComposeFunc func(msg bh.Msg, ctx bh.RcvContext, err error) Step
