package beehive

import (
	"encoding/gob"
	"errors"
	"fmt"
	"sync"

	"github.com/soheilhy/beehive/Godeps/_workspace/src/github.com/coreos/etcd/raft/raftpb"
	"github.com/soheilhy/beehive/Godeps/_workspace/src/github.com/golang/glog"
	bhgob "github.com/soheilhy/beehive/gob"
	"github.com/soheilhy/beehive/raft"
)

var (
	ErrUnsupportedRequest = errors.New("Unsupported request")
	ErrInvalidParam       = errors.New("Invalid parameter")
	ErrNoSuchHive         = errors.New("No such hive")
	ErrDuplicateHive      = errors.New("Dupblicate hive")
	ErrNoSuchBee          = errors.New("No such bee")
	ErrDuplicateBee       = errors.New("Duplicate bee")
)

// noOp is a barrier: a raft request to make sure all the updates are
// applied to store.
type noOp struct{}

// newHiveID is the registry request to create a unique 64-bit hive ID.
type newHiveID struct {
	Addr string
}

// newBeeID is the registry request to create a unique 64-bit bee ID.
type newBeeID struct{}

// BeeInfo stores the metadata about a bee.
type BeeInfo struct {
	ID       uint64
	Hive     uint64
	App      string
	Colony   Colony
	Detached bool
}

// addBee is the registery request to add a new bee.
type addBee BeeInfo

// delBee is the registery request to delete a bee.
type delBee uint64

// moveBee is the registery request to move a bee from a hive to another hive.
type moveBee struct {
	ID       uint64
	FromHive uint64
	ToHive   uint64
}

// Colony is a registery request to update a colony.
type updateColony struct {
	Old Colony
	New Colony
}

// LockMappedCell locks a mapped cell for a colony.
type LockMappedCell struct {
	Colony Colony
	App    string
	Cells  MappedCells
}

// TransferLocks transfers cells of a colony to another colony.
type transferCells struct {
	From Colony
	To   Colony
}

func init() {
	gob.Register(noOp{})
	gob.Register(newBeeID{})
	gob.Register(newHiveID{})
	gob.Register(HiveInfo{})
	gob.Register([]HiveInfo{})
	gob.Register(BeeInfo{})
	gob.Register(addBee{})
	gob.Register(delBee(0))
	gob.Register(updateColony{})
	gob.Register(LockMappedCell{})
	gob.Register(transferCells{})
	gob.Register(cellStore{})
}

type registry struct {
	m sync.RWMutex

	HiveID uint64
	BeeID  uint64
	Hives  map[uint64]HiveInfo
	Bees   map[uint64]BeeInfo
	Store  cellStore
}

func newRegistry() *registry {
	return &registry{
		HiveID: 1, // We need to start from one to preserve the first hive's ID.
		BeeID:  0,
		Hives:  make(map[uint64]HiveInfo),
		Bees:   make(map[uint64]BeeInfo),
		Store:  newCellStore(),
	}
}

func (r *registry) Save() ([]byte, error) {
	r.m.RLock()
	defer r.m.RUnlock()
	return bhgob.Encode(r)
}

func (r *registry) Restore(b []byte) error {
	r.m.Lock()
	defer r.m.Unlock()
	return bhgob.Decode(r, b)
}

func (r *registry) Apply(req interface{}) (interface{}, error) {
	r.m.Lock()
	defer r.m.Unlock()

	switch tr := req.(type) {
	case noOp:
		return nil, nil
	case newHiveID:
		return r.newHiveID(tr.Addr), nil
	case newBeeID:
		return r.newBeeID(), nil
	case addBee:
		return nil, r.addBee(BeeInfo(tr))
	case delBee:
		return nil, r.delBee(uint64(tr))
	case moveBee:
		return nil, r.moveBee(tr)
	case LockMappedCell:
		return r.lock(tr)
	case transferCells:
		return nil, r.transfer(tr)
	}

	return nil, ErrUnsupportedRequest
}

func (r *registry) ApplyConfChange(cc raftpb.ConfChange,
	n raft.NodeInfo) error {

	switch cc.Type {
	case raftpb.ConfChangeAddNode:
		if n.ID != cc.NodeID {
			glog.Fatalf("Invalid data in the config change: %v != %v", n, cc.NodeID)
		}
		if n.Addr != "" {
			r.addHive(HiveInfo(n))
		}
		glog.V(2).Infof("Hive added %v@%v", n.ID, n.Addr)

	case raftpb.ConfChangeRemoveNode:
		r.delHive(cc.NodeID)
		glog.V(2).Infof("Hive %v deleted", cc.NodeID)
	}
	return nil
}

func (r *registry) newHiveID(addr string) uint64 {
	r.HiveID++
	if addr != "" {
		r.addHive(HiveInfo{ID: r.HiveID, Addr: addr})
	}
	glog.V(2).Infof("Registry allocates new hive ID %v", r.HiveID)
	return r.HiveID
}

func (r *registry) newBeeID() uint64 {
	r.BeeID++
	glog.V(2).Infof("Registry allocates new bee ID %v", r.BeeID)
	return r.BeeID
}

func (r *registry) delHive(id uint64) error {
	if _, ok := r.Hives[id]; !ok {
		return fmt.Errorf("No such hive %v", id)
	}
	delete(r.Hives, id)
	return nil
}

func (r *registry) initHives(hives map[uint64]HiveInfo) error {
	r.m.Lock()
	defer r.m.Unlock()

	for _, h := range hives {
		if err := r.addHive(h); err != nil {
			return err
		}
	}
	return nil
}

func (r *registry) addHive(info HiveInfo) error {
	glog.V(2).Infof("hive %v's address is set to %v", info.ID, info.Addr)
	for _, h := range r.Hives {
		if h.Addr == info.Addr && h.ID != info.ID {
			glog.Fatalf("duplicate address %v for hives %v and %v", info.Addr,
				info.ID, h.ID)
		}
	}
	r.Hives[info.ID] = info
	return nil
}

func (r *registry) addBee(info BeeInfo) error {
	if _, ok := r.Bees[info.ID]; ok {
		return ErrDuplicateBee
	}
	if info.ID < r.BeeID {
		glog.Fatalf("Invalid bee ID: %v < %v", info.ID, r.HiveID)
	}
	r.Bees[info.ID] = info
	return nil
}

func (r *registry) delBee(id uint64) error {
	if _, ok := r.Bees[id]; !ok {
		return ErrNoSuchBee
	}
	delete(r.Bees, id)
	return nil
}

func (r *registry) moveBee(m moveBee) error {
	b, ok := r.Bees[m.ID]
	if !ok {
		return ErrNoSuchBee
	}

	if b.Hive != m.FromHive {
		return ErrInvalidParam
	}

	if m.FromHive == m.ToHive {
		return nil
	}

	b.Hive = m.ToHive
	r.Bees[m.ID] = b
	return nil
}

func (r *registry) lock(l LockMappedCell) (Colony, error) {
	if l.Colony.Leader == 0 {
		return Colony{}, ErrInvalidParam
	}

	locked := false
	openk := make(MappedCells, 0, 10)
	for _, k := range l.Cells {
		c, ok := r.Store.colony(l.App, k)
		if !ok {
			if locked {
				r.Store.assign(l.App, k, l.Colony)
			} else {
				openk = append(openk, k)
			}
			continue
		}

		if locked && !c.Equal(l.Colony) {
			// FIXME(soheil): Fix this bug after refactoring the code.
			glog.Fatal("TODO registery conflict between colonies")
		}

		locked = true
		l.Colony = c
	}

	if locked {
		for _, k := range openk {
			r.Store.assign(l.App, k, l.Colony)
		}
		return l.Colony, nil
	}

	for _, k := range l.Cells {
		r.Store.assign(l.App, k, l.Colony)
	}
	return l.Colony, nil
}

func (r *registry) transfer(t transferCells) error {
	i, ok := r.Bees[t.From.Leader]
	if !ok {
		return ErrNoSuchBee
	}
	keys := r.Store.cells(t.From.Leader)
	if len(keys) == 0 {
		return ErrInvalidParam
	}
	for _, k := range keys {
		r.Store.assign(i.App, k, t.To)
	}
	return nil
}

func (r *registry) hives() []HiveInfo {
	r.m.RLock()
	defer r.m.RUnlock()
	hives := make([]HiveInfo, 0, len(r.Hives))
	for _, h := range r.Hives {
		hives = append(hives, h)
	}
	return hives
}

func (r *registry) hive(id uint64) (HiveInfo, error) {
	r.m.RLock()
	defer r.m.RUnlock()
	i, ok := r.Hives[id]
	if !ok {
		return i, ErrNoSuchHive
	}
	return i, nil
}

func (r *registry) beesOfHive(id uint64) []BeeInfo {
	r.m.RLock()
	defer r.m.RUnlock()
	var bees []BeeInfo
	for _, b := range r.Bees {
		if b.Hive == id {
			bees = append(bees, b)
		}
	}
	return bees
}

func (r *registry) bee(id uint64) (BeeInfo, error) {
	r.m.RLock()
	defer r.m.RUnlock()
	i, ok := r.Bees[id]
	if !ok {
		return i, ErrNoSuchBee
	}
	return i, nil
}

func (r *registry) beeForCells(app string, cells MappedCells) (info BeeInfo,
	hasAll bool, err error) {

	r.m.RLock()
	defer r.m.RUnlock()

	hasAll = true
	for _, k := range cells {
		col, ok := r.Store.colony(app, k)
		if !ok {
			hasAll = false
			continue
		}

		if info.ID == 0 {
			info = r.Bees[col.Leader]
		} else if info.ID != col.Leader {
			// Incosistencies should be handled by consensus.
			hasAll = false
		}

		if !hasAll {
			return info, hasAll, nil
		}
	}
	if info.ID == 0 {
		return info, hasAll, ErrNoSuchBee
	}
	return info, hasAll, nil
}
