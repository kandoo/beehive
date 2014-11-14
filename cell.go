package beehive

import "fmt"

// CellKey represents a key in a dictionary.
type CellKey struct {
	Dict string
	Key  string
}

// AppCellKey represents a key in a dictionary of a specific app.
type AppCellKey struct {
	App  string
	Dict string
	Key  string
}

// Cell returns the CellKey of this AppCellKey.
func (ack AppCellKey) Cell() CellKey {
	return CellKey{
		Dict: ack.Dict,
		Key:  ack.Key,
	}
}

// IsNil returns whether the AppCellKey represents no cells.
func (ack AppCellKey) IsNil() bool {
	return ack.App == ""
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
	// appname -> dict -> key -> colony
	CellBees map[string]map[string]map[string]Colony
	// beeid -> dict -> key
	BeeCells map[uint64]map[string]map[string]struct{}
}

func newCellStore() cellStore {
	return cellStore{
		CellBees: make(map[string]map[string]map[string]Colony),
		BeeCells: make(map[uint64]map[string]map[string]struct{}),
	}
}

func (s *cellStore) assign(app string, k CellKey, c Colony) {
	s.assignCellBees(app, k, c)
	s.assignBeeCells(app, k, c)
}

func (s *cellStore) assignCellBees(app string, k CellKey, c Colony) {
	dicts, ok := s.CellBees[app]
	if !ok {
		dicts = make(map[string]map[string]Colony)
		s.CellBees[app] = dicts
	}
	keys, ok := dicts[k.Dict]
	if !ok {
		keys = make(map[string]Colony)
		dicts[k.Dict] = keys
	}
	keys[k.Key] = c
}

func (s *cellStore) assignBeeCells(app string, k CellKey, c Colony) {
	dicts, ok := s.BeeCells[c.Leader]
	if !ok {
		dicts = make(map[string]map[string]struct{})
		s.BeeCells[c.Leader] = dicts
	}
	keys, ok := dicts[k.Dict]
	if !ok {
		keys = make(map[string]struct{})
		dicts[k.Dict] = keys
	}
	keys[k.Key] = struct{}{}
}

func (s *cellStore) colony(app string, cell CellKey) (c Colony, ok bool) {
	dicts, ok := s.CellBees[app]
	if !ok {
		return Colony{}, false
	}
	keys, ok := dicts[cell.Dict]
	if !ok {
		return Colony{}, false
	}
	c, ok = keys[cell.Key]
	return c, ok
}

func (s *cellStore) cells(bee uint64) MappedCells {
	dicts, ok := s.BeeCells[bee]
	if !ok {
		return nil
	}
	c := make(MappedCells, 0, len(dicts))
	for d, dict := range dicts {
		for k := range dict {
			c = append(c, CellKey{Dict: d, Key: k})
		}
	}
	return c
}

func (s *cellStore) updateColony(app string, oldc Colony, newc Colony) error {
	bdicts := s.BeeCells[oldc.Leader]
	if oldc.Leader != newc.Leader {
		s.BeeCells[newc.Leader] = bdicts
		delete(s.BeeCells, oldc.Leader)
	}
	acells := s.CellBees[app]
	for d, dict := range bdicts {
		for k := range dict {
			acells[d][k] = newc
		}
	}
	return nil
}
