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

// NoOp is a barrier: a registery request to make sure all the updates are
// applied to store.
type NoOp struct{}

// NewHiveID is the registry request to create a unique 64-bit hive ID.
type NewHiveID struct {
	Addr string
}

// NewBeeID is the registry request to create a unique 64-bit bee ID.
type NewBeeID struct{}

// BeeInfo stores the metadata about a bee.
type BeeInfo struct {
	ID       uint64
	Hive     uint64
	App      string
	Colony   Colony
	Detached bool
}

// AddBee is the registery request to add a new bee.
type AddBee BeeInfo

// DelBee is the registery request to delete a bee.
type DelBee uint64

// MoveBee is the registery request to move a bee from a hive to another hive.
type MoveBee struct {
	ID       uint64
	FromHive uint64
	ToHive   uint64
}

// Colony is a registery request to update a colony.
type UpdateColony struct {
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
type TransferCells struct {
	From Colony
	To   Colony
}

func init() {
	gob.Register(NoOp{})
	gob.Register(NewBeeID{})
	gob.Register(NewHiveID{})
	gob.Register(HiveInfo{})
	gob.Register([]HiveInfo{})
	gob.Register(BeeInfo{})
	gob.Register(AddBee{})
	gob.Register(DelBee(0))
	gob.Register(UpdateColony{})
	gob.Register(LockMappedCell{})
	gob.Register(TransferCells{})
	gob.Register(CellStore{})
}

type registry struct {
	m sync.RWMutex

	HiveID uint64
	BeeID  uint64
	Hives  map[uint64]HiveInfo
	Bees   map[uint64]BeeInfo
	Store  CellStore
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
	case NoOp:
		return nil, nil
	case NewHiveID:
		return r.newHiveID(tr.Addr), nil
	case NewBeeID:
		return r.newBeeID(), nil
	case AddBee:
		return nil, r.addBee(BeeInfo(tr))
	case DelBee:
		return nil, r.delBee(uint64(tr))
	case MoveBee:
		return nil, r.moveBee(tr)
	case LockMappedCell:
		return r.lock(tr)
	case TransferCells:
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
	glog.V(2).Infof("Registry allocate new hive ID %v", r.HiveID)
	return r.HiveID
}

func (r *registry) newBeeID() uint64 {
	r.BeeID++
	return r.BeeID
}

func (r *registry) delHive(id uint64) error {
	if _, ok := r.Hives[id]; !ok {
		return fmt.Errorf("No such hive %v", id)
	}
	delete(r.Hives, id)
	return nil
}

func (r *registry) addHive(info HiveInfo) error {
	if r.HiveID < info.ID {
		//glog.Fatalf("Hive ID %v is after allocated hive id", r.HiveID, info.ID)
	}
	glog.V(2).Infof("Hive %v's address is set to %v", info.ID, info.Addr)
	r.Hives[info.ID] = info
	return nil
}

func (r *registry) addBee(info BeeInfo) error {
	if _, ok := r.Bees[info.ID]; ok {
		return ErrDuplicateBee
	}
	if info.ID < r.HiveID {
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

func (r *registry) moveBee(m MoveBee) error {
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

func (r *registry) transfer(t TransferCells) error {
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
	hives := make([]HiveInfo, len(r.Hives))
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
