package bh

import (
	"bytes"
	"encoding/gob"
)

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

	// GetGob retrieves the value stored for k in d, and decodes it into v using
	// gob. Returns error when there is no value or when it cannot decode it.
	GetGob(k Key, v interface{}) error
	// PutGob encodes v using gob and store it for key k in d.
	PutGob(k Key, v interface{}) error
}

// DictName is the key to lookup dictionaries in the state.
type DictName string

// Key is to lookup values in Dicitonaries and is simply a string.
type Key string

// Value represents a value in Dict.
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

func GetGob(d Dict, k Key, v interface{}) error {
	dv, err := d.Get(k)
	if err != nil {
		return err
	}
	buf := bytes.NewBuffer(dv)
	dec := gob.NewDecoder(buf)
	return dec.Decode(v)
}

func PutGob(d Dict, k Key, v interface{}) error {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(v); err != nil {
		return err
	}
	d.Put(k, buf.Bytes())
	return nil
}
