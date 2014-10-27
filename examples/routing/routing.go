package routing

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/golang/glog"
	bh "github.com/kandoo/beehive"
)

// InstallRouting installs the routing application on bh.DefaultHive.
// timeout is the duration between each epoc of routing advertisements.
func InstallRouting(timeout time.Duration) {
	app := bh.NewApp("Routing")
	router := Router{}
	app.Handle(Advertisement{}, router)
	app.Handle(Discovery{}, router)
	app.Handle(Timeout{}, router)
	go func() {
		ticker := time.NewTicker(timeout)
		for {
			<-ticker.C
			bh.Emit(Timeout{})
		}
	}()
}

// InstallRoutingOnHive install the routing application
func InstallRoutingOnHive(h bh.Hive, timeout time.Duration) {
	app := h.NewApp("Routing")
	router := Router{}
	app.Handle(Advertisement{}, router)
	app.Handle(Discovery{}, router)
	app.Handle(Timeout{}, router)
	go func() {
		ticker := time.NewTicker(timeout)
		for {
			<-ticker.C
			h.Emit(Timeout{})
		}
	}()
}

// Route is a collection of paths towards a desintation node.
type Route struct {
	To      Node
	Paths   []Path // Paths is sorted based on Path.Len().
	Updates []Path // The new routes that may be used for advertisement.
}

// Contains returns whether route already contains path.
func (r Route) Contains(path Path) bool {
	for _, p := range r.Paths {
		if p.Equal(path) {
			return true
		}
	}
	return false
}

// Routing table represents the route for each destination node.
type RoutingTable map[Node]Route

// Encode encodes routes into bytes using GOB.
func (r RoutingTable) Encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(r); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes routes from bytes using GOB.
func (r *RoutingTable) Decode(b []byte) error {
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	return dec.Decode(r)
}

// PathByLen is used to sort a []Path based on the length of paths. It
// implements sort.Interface for []Path.
type PathByLen []Path

func (r PathByLen) Len() int           { return len(r) }
func (r PathByLen) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }
func (r PathByLen) Less(i, j int) bool { return r[i].Len() < r[j].Len() }

// AddPath adds a path to this route.
func (r *Route) AddPath(p Path) error {
	if to, err := p.To(); err != nil || to != r.To {
		return fmt.Errorf("Path is towards %v not %v", to, r.To)
	}

	r.Paths = append(r.Paths, p)
	sort.Sort(PathByLen(r.Paths))
	return nil
}

// RemovePath removes a path from this route.
func (r *Route) RemovePath(p Path) error {
	if to, err := p.To(); err != nil || to != r.To {
		return fmt.Errorf("Path is towards %v not %v", to, r.To)
	}

	for i, rp := range r.Paths {
		if rp.Equal(p) {
			r.Paths = append(r.Paths[:i], r.Paths[i+1:]...)
			sort.Sort(PathByLen(r.Paths))
			return nil
		}
	}

	return fmt.Errorf("Paths %v is not found in this route", p)
}

// KShortestPaths returns the k shortest paths in this route. If k is larger
// than the number of paths we have, we return all the paths.
func (r Route) KShortestPaths(k int) []Path {
	if l := len(r.Paths); l < k {
		k = l
	}
	return r.Paths[:k]
}

// ShortestPaths returns all the paths of shortest length in this route.
func (r Route) ShortestPaths() []Path {
	p := PathByLen(r.Paths)
	for i := 0; i < len(p)-1; i++ {
		if p.Less(i, i+1) {
			return r.Paths[0:i]
		}
	}
	return r.Paths
}

// IsShortestPath returns whether path is a shortest path.
func (r Route) IsShortestPath(path Path) bool {
	p := PathByLen(r.Paths)
	for i := 0; i < len(p); i++ {
		if p[i].Equal(path) {
			return true
		}
		if i < len(p)-1 && p.Less(i, i+1) {
			return false
		}
	}
	return false
}

// Discovery is a message emitted when an edge between two nodes is discovered.
type Discovery Edge

// Advertisement is a single route advertisement.
type Advertisement Path

// Timeout is a message emitted when the router should advertise new routes to
// its neighbors.
type Timeout struct{}

// Router is the main handler of the routing application.
type Router struct{}

// Dictionaries used by Router.
const (
	neighDict = "Neighbors"
	routeDict = "Routes"
)

// Rcv handles both Discovery and Advertisement messages.
func (r Router) Rcv(msg bh.Msg, ctx bh.RcvContext) error {
	switch d := msg.Data().(type) {
	case Discovery:
		if err := r.appendNieghbor(Edge(d), ctx); err != nil {
			return err
		}
		if Edge(d).To.Endhost {
			adv, err := Path{}.Append(Edge(d).From, Edge(d).To)
			if err != nil {
				return err
			}
			ctx.Emit(Advertisement(adv))
		}
		// Indication of an update.
		ctx.SetBeeLocal(true)

	case Advertisement:
		path := Path(d)
		if to, err := path.To(); err != nil || !to.Endhost {
			return errors.New("Route is not towards an end-host")
		}
		if _, err := r.appendToRoutingTable(path, ctx); err != nil {
			return err
		}

	case Timeout:
		if !ctx.BeeLocal().(bool) {
			return nil
		}
		ctx.SetBeeLocal(false)

		ctx.Dict(routeDict).ForEach(func(k string, v []byte) {
			var tbl RoutingTable
			if err := tbl.Decode(v); err != nil {
				return
			}

			from := Node{
				ID: string(k),
			}

			for to, route := range tbl {
				if len(route.Updates) == 0 {
					continue
				}

				shortestPaths := route.ShortestPaths()
				glog.V(1).Infof("Shortest paths: %v", shortestPaths)
				for _, p := range route.Updates {
					if !route.IsShortestPath(p) {
						continue
					}

					for _, n := range r.neighbors(from, ctx) {
						if np, err := p.Prepend(n.From); err == nil {
							ctx.Emit(Advertisement(np))
						}
					}
				}
				route.Updates = nil
				tbl[to] = route
			}

			if v, err := tbl.Encode(); err == nil {
				ctx.Dict(routeDict).Put(k, v)
			}
		})
	}

	return nil
}

// Rcv maps Discovery based on its destination node and Advertisement messages
// based on their source node.
func (r Router) Map(msg bh.Msg, ctx bh.MapContext) bh.MappedCells {
	switch d := msg.Data().(type) {
	case Discovery:
		return bh.MappedCells{{neighDict, d.To.Key()}, {routeDict, d.To.Key()}}
	case Advertisement:
		from, err := Path(d).From()
		if err != nil {
			return nil
		}
		return bh.MappedCells{{neighDict, from.Key()}, {routeDict, from.Key()}}
	case Timeout:
		return bh.MappedCells{}
	}
	return nil
}

func (r Router) neighbors(node Node, ctx bh.RcvContext) Edges {
	dict := ctx.Dict(neighDict)
	var neighs Edges
	if v, err := dict.Get(node.Key()); err == nil {
		neighs.Decode(v)
	}
	return neighs
}

func (r Router) appendNieghbor(edge Edge, ctx bh.RcvContext) error {
	neighs := r.neighbors(edge.To, ctx)
	if neighs.Contains(edge) {
		return fmt.Errorf("%v is already a neighbor", edge)
	}
	neighs = append(neighs, edge)
	b, err := neighs.Encode()
	if err != nil {
		return err
	}
	ctx.Dict(neighDict).Put(edge.To.Key(), b)
	return nil
}

func (r Router) routingTable(from Node, ctx bh.RcvContext) RoutingTable {
	dict := ctx.Dict(routeDict)
	var tbl RoutingTable
	if v, err := dict.Get(from.Key()); err == nil {
		tbl.Decode(v)
	} else {
		tbl = make(RoutingTable)
	}
	return tbl
}

func (r Router) appendToRoutingTable(path Path, ctx bh.RcvContext) (Route,
	error) {

	from, err := path.From()
	if err != nil {
		return Route{}, err
	}

	to, err := path.To()
	if err != nil {
		return Route{}, err
	}

	tbl := r.routingTable(from, ctx)
	route, ok := tbl[to]
	if !ok {
		route = Route{
			To: to,
		}
	}

	if route.Contains(path) {
		return route, errors.New("Route already has the path")
	}

	if err := route.AddPath(path); err != nil {
		return route, err
	}

	if route.IsShortestPath(path) {
		route.Updates = append(route.Updates, path)
		// Indication of an update in bee local.
		ctx.SetBeeLocal(true)
	}

	tbl[to] = route
	b, err := tbl.Encode()
	if err != nil {
		return route, err
	}

	ctx.Dict(routeDict).Put(from.Key(), b)
	return route, nil
}
