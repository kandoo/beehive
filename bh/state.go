package bh

// State is the storage for a collection of dictionaries. State support
// transactions. However, note that since the state is modified by a single
// thread, transactions are there mostly for programming convenience. For the
// same reason, each thread can only have one active transaction.
type State interface {
	// Returns a dictionary for this state. Creates one if it does not exist.
	Dict(name DictionaryName) Dictionary
	// Starts a transaction for this state. Transactions span multiple
	// dictionaries.
	StartTx() error
	// Commits the current transaction.
	CommitTx() error
	// Aborts the transaction.
	AbortTx() error
}

type IterFn func(k Key, v Value)

// Simply a key-value store.
type Dictionary interface {
	Get(k Key) (Value, error)
	Put(k Key, v Value) error
	Del(k Key) error
	ForEach(f IterFn)
}

// DictionaryName is the key to lookup dictionaries in the state.
type DictionaryName string

// Key is to lookup values in Dicitonaries and is simply a string.
type Key string

// Dictionary values can be anything.
type Value []byte

type DictionaryKey struct {
	Dict DictionaryName
	Key  Key
}

func (dk *DictionaryKey) String() string {
	// TODO(soheil): This will change when we implement it using a Trie instead of
	// a map.
	return string(dk.Dict) + "/" + string(dk.Key)
}

func newState(name string) State {
	return &inMemoryState{name, make(map[string]*inMemoryDictionary)}
}
