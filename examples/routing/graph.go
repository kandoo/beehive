package routing

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"

	"github.com/soheilhy/beehive/bh"
)

// Node represents a vertex in the graph.
type Node struct {
	ID string // ID of this node.
}

func (n Node) String() string {
	return n.ID
}

// Key returns the beehive key representing this node.
func (n Node) Key() bh.Key {
	return bh.Key(n.String())
}

// Edge represents a driected edge between two nodes.
type Edge struct {
	From Node
	To   Node
}

// Edges is an alias for a slice of edges.
type Edges []Edge

// Encode encodes an edge into bytes using Gob.
func (s Edges) Encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(&s); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes an edge from bytes using Gob.
func (s *Edges) Decode(b []byte) error {
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	return dec.Decode(s)
}

// Contains returns whether the edges contain edge.
func (s Edges) Contains(edge Edge) bool {
	for _, e := range s {
		if e == edge {
			return true
		}
	}
	return false
}

// Path represents a directed path between two nodes.
type Path []Node

// Equal checks the equality of paths.
func (p Path) Equal(thatp Path) bool {
	if p.Len() != thatp.Len() {
		return false
	}

	for i := range p {
		if p[i] != thatp[i] {
			return false
		}
	}

	return true
}

// Len returns the length of the path.
func (p Path) Len() int {
	return len(p)
}

// Valid returns true if the path is a valid path. It checks the length of the
// path and also detects if the path has a loop.
func (p Path) Valid() bool {
	if p.Len() <= 0 {
		return false
	}

	nmap := make(map[Node]bool)
	for _, n := range p {
		if nmap[n] {
			return false
		}
		nmap[n] = true
	}
	return true
}

// To returns the destination node of the path.
func (p Path) To() (Node, error) {
	if !p.Valid() {
		return Node{}, errors.New("This is an invalid path")
	}

	return p[p.Len()-1], nil
}

// From returns the source node of the path.
func (p Path) From() (Node, error) {
	if !p.Valid() {
		return Node{}, errors.New("This is an invalid path")
	}

	return p[0], nil
}

// Append creates a copy of the path, appends n to the path, and returns
// that copy. It will not directly modify the path. It returns error if the
// resulting path is an invalid path.
func (p Path) Append(n ...Node) (Path, error) {
	c := make(Path, p.Len()+len(n))
	copy(c, p)
	copy(c[p.Len():], n)
	if !c.Valid() {
		return c, fmt.Errorf("This is an invalid path %v", c)
	}
	return c, nil
}

// Prepend creates a copy of the path, prepends n to the path, and returns
// that copy. It will not directly modify the path. It returns error if the
// resulting path is an invalid path.
func (p Path) Prepend(n ...Node) (Path, error) {
	c := make(Path, p.Len()+len(n))
	copy(c, n)
	copy(c[len(n):], p)
	if !c.Valid() {
		return c, fmt.Errorf("This is an invalid path %v", c)
	}

	return c, nil
}
