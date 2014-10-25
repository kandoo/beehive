package beehive

import "fmt"

// CellKey represents a key in a dictionary.
type CellKey struct {
	Dict string
	Key  string
}

// This is the list of dictionary keys returned by the map functions.
type MappedCells []CellKey

func (mc MappedCells) String() string {
	return fmt.Sprintf("Cells{%v}", []CellKey(mc))
}

func (mc MappedCells) Len() int      { return len(mc) }
func (mc MappedCells) Swap(i, j int) { mc[i], mc[j] = mc[j], mc[i] }
func (mc MappedCells) Less(i, j int) bool {
	return mc[i].Dict < mc[j].Dict ||
		(mc[i].Dict == mc[j].Dict && mc[i].Key < mc[j].Key)
}

// An empty mapset means a local broadcast of message.
func (mc MappedCells) LocalBroadcast() bool {
	return len(mc) == 0
}

type cellStore struct {
	// appname -> cellkey -> colony
	CellBees map[string]map[CellKey]Colony
	// beeid -> cellkey
	BeeCells map[uint64]map[CellKey]struct{}
}

func newCellStore() cellStore {
	return cellStore{
		CellBees: make(map[string]map[CellKey]Colony),
		BeeCells: make(map[uint64]map[CellKey]struct{}),
	}
}

func (s *cellStore) assign(app string, k CellKey, c Colony) {
	cells, ok := s.CellBees[app]
	if !ok {
		cells = make(map[CellKey]Colony)
		s.CellBees[app] = cells
	}
	cells[k] = c

	m, ok := s.BeeCells[c.Leader]
	if !ok {
		m = make(map[CellKey]struct{})
		s.BeeCells[c.Leader] = m
	}
	m[k] = struct{}{}
}

func (s *cellStore) colony(app string, cell CellKey) (c Colony, ok bool) {
	cells, ok := s.CellBees[app]
	if !ok {
		return Colony{}, ok
	}

	c, ok = cells[cell]
	return c, ok
}

func (s *cellStore) cells(bee uint64) MappedCells {
	m, ok := s.BeeCells[bee]
	if !ok {
		return nil
	}
	c := make(MappedCells, 0, len(m))
	for k := range m {
		c = append(c, k)
	}
	return c
}
