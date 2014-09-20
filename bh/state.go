package bh

// State is the storage for a collection of dictionaries.
type State interface {
	// Returns a dictionary for this state. Creates one if it does not exist.
	Dict(name DictName) Dict
}

// TxState represents a transactional state.
type TxState interface {
	State
	// Starts a transaction for this state. Transactions span multiple
	// dictionaries.
	BeginTx() error
	// Commits the current transaction.
	CommitTx() error
	// Aborts the transaction.
	AbortTx() error
	// Tx returns all the operations in the transaction.
	Tx() []StateOp
}

type IterFn func(k Key, v Value)

// Simply a key-value store.
type Dict interface {
	Name() DictName
	Get(k Key) (Value, error)
	Put(k Key, v Value) error
	Del(k Key) error
	ForEach(f IterFn)
}

// DictName is the key to lookup dictionaries in the state.
type DictName string

// Key is to lookup values in Dicitonaries and is simply a string.
type Key string

// Dict values can be anything.
type Value []byte

type CellKey struct {
	Dict DictName
	Key  Key
}

func (dk *CellKey) String() string {
	// TODO(soheil): This will change when we implement it using a Trie instead of
	// a map.
	return string(dk.Dict) + "/" + string(dk.Key)
}

func newState(a *app) TxState {
	if a.Persistent() {
		return a.hive.stateMan.newState(a)
	}

	return &inMemoryState{
		Name:  string(a.Name()),
		Dicts: make(map[DictName]*inMemDict),
	}
}

type OpType int

const (
	Unknown OpType = iota
	Put            = iota
	Del            = iota
)

type StateOp struct {
	T OpType
	D DictName
	K Key
	V Value
}
