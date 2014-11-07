package composition

import (
	bh "github.com/kandoo/beehive"
	"github.com/kandoo/beehive/state"
)

type composedRcvContext struct {
	bh.RcvContext
	prefix string
}

func (c composedRcvContext) Dict(name string) state.Dict {
	return c.RcvContext.Dict(c.prefix + name)
}

type composedMapContext struct {
	bh.MapContext
	prefix string
}

func (c composedMapContext) Dict(name string) state.Dict {
	return c.MapContext.Dict(c.prefix + name)
}
