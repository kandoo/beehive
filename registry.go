package beehive

import (
	"encoding/gob"
	"errors"
	"fmt"
	"sync"

	"github.com/golang/glog"
	bhgob "github.com/soheilhy/beehive/gob"
)

var (
	ErrUnsupportedRequest = errors.New("Unsupported request")
	ErrInvalidParam       = errors.New("Invalid parameter")
	ErrNoSuchHive         = errors.New("No such hive")
	ErrDuplicateHive      = errors.New("Dupblicate hive")
	ErrNoSuchBee          = errors.New("No such bee")
	ErrDuplicateBee       = errors.New("Duplicate bee")
)

// HiveJoined is emitted when a hive joins the cluster. Note that this message
// is emitted on all hives.
type HiveJoined struct {
	ID   uint64 // The ID of the hive.
	Addr string // Connection addr of the hive.
}

// HiveLeft is emitted when a hive leaves the cluster. Note that this event is
// emitted on all hives.
type HiveLeft struct {
	ID uint64 // The ID of the hive.
}

// BeeMoved is emitted when a bee is moved from a hive to another hive.
type BeeMoved MoveBee

// CellLocked is when a cell is successfully locked by a colony.
type CellsLocked struct {
	Colony Colony
	App    string
	Cells  MappedCells
}

// HiveInfo stores the ID and the address of a hive.
type HiveInfo struct {
	ID   uint64
	Addr string
}

// NewHiveID is the registry request to create a unique 64-bit hive ID.
type NewHiveID struct{}

// NewBeeID is the registry request to create a unique 64-bit bee ID.
type NewBeeID struct{}

// AddHive is the registery request to add a hive.
type AddHive HiveInfo

// DelHive is the registery request to delete a hive.
type DelHive uint64

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
	gob.Register(HiveJoined{})
	gob.Register(HiveLeft{})
	gob.Register(HiveInfo{})
	gob.Register(AddHive{})
	gob.Register(DelHive(0))
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
	e Emitter

	HiveID uint64
	BeeID  uint64
	Hives  map[uint64]HiveInfo
	Bees   map[uint64]BeeInfo
	Store  CellStore
}

func newRegistry(e Emitter) *registry {
	return &registry{
		e:      e,
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

func (r *registry) Apply(req interface{}) interface{} {
	r.m.Lock()
	defer r.m.Unlock()

	switch tr := req.(type) {
	case NewHiveID:
		return r.newHiveID()
	case NewBeeID:
		return r.newBeeID()
	case AddHive:
		return r.addHive(HiveInfo(tr))
	case DelHive:
		return r.deleteHive(uint64(tr))
	case AddBee:
		return r.addBee(BeeInfo(tr))
	case DelBee:
		return r.delBee(uint64(tr))
	case MoveBee:
		return r.moveBee(tr)
	case LockMappedCell:
		return r.lock(tr)
	case TransferCells:
		return r.transfer(tr)
	}

	return ErrUnsupportedRequest
}

func (r *registry) newHiveID() uint64 {
	r.HiveID++
	return r.HiveID
}

func (r *registry) newBeeID() uint64 {
	r.BeeID++
	return r.BeeID
}

func (r *registry) deleteHive(id uint64) error {
	if _, ok := r.Hives[id]; !ok {
		return fmt.Errorf("No such hive %v", id)
	}
	delete(r.Hives, id)
	go r.e.Emit(HiveLeft{ID: id})
	return nil
}

func (r *registry) addHive(info HiveInfo) error {
	if _, ok := r.Hives[info.ID]; ok {
		return ErrDuplicateHive
	}
	if info.ID < r.HiveID {
		glog.Fatalf("Invalid hive ID: %v < %v", info.ID, r.HiveID)
	}
	r.Hives[info.ID] = info
	go r.e.Emit(HiveJoined(info))
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
	go r.e.Emit(BeeMoved(m))
	return nil
}

func (r *registry) lock(l LockMappedCell) Colony {
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
		go r.e.Emit(CellsLocked{Colony: l.Colony, App: l.App, Cells: l.Cells})
		return l.Colony
	}

	for _, k := range l.Cells {
		r.Store.assign(l.App, k, l.Colony)
	}
	go r.e.Emit(CellsLocked{Colony: l.Colony, App: l.App, Cells: l.Cells})
	return l.Colony
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
	go r.e.Emit(CellsLocked{Colony: t.To, App: i.App, Cells: keys})
	return nil
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
