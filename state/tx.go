package state

import (
	"errors"
	"fmt"
)

// TxStatus represents the status of a transaction.
type TxStatus uint8

// Valid values for TxStatus
const (
	TxNone TxStatus = iota
	TxOpen          = iota
)

// Tx represents the side effects of an operation: messages emitted during the
// transaction as well as state operations.
type Tx struct {
	Ops    []Op     // State operations in this tx.
	Status TxStatus // Status of this tx.
}

// AddOp adds a state operation to this transaction.
func (t *Tx) AddOp(op Op) {
	t.Ops = append(t.Ops, op)
}

// IsOpen returns true if the transaction is open.
func (t *Tx) IsOpen() bool {
	return t.Status == TxOpen
}

// Reset simply clears operations of the transaction.
func (t *Tx) Reset() {
	t.Status = TxNone
	if !t.IsEmpty() {
		t.Ops = nil
	}
}

// IsEmpty returns true if there is no operations in the tx.
func (t *Tx) IsEmpty() bool {
	return len(t.Ops) == 0
}
func (t Tx) String() string {
	return fmt.Sprintf("Tx (ops: %d, open: %v)", len(t.Ops), t.Status == TxOpen)
}

// NewTransactional wraps s and writtens a transactional state.
func NewTransactional(s State) *Transactional {
	return &Transactional{State: s}
}

// Transactional wraps any state dictionary and makes it transactional.
type Transactional struct {
	State  State
	stage  map[string]*stageDict
	status TxStatus
}

type stageDict struct {
	dict Dict
	ops  map[string]Op
}

func (t *Transactional) TxStatus() TxStatus {
	return t.status
}

func (t *Transactional) BeginTx() error {
	if t.status == TxOpen {
		return errors.New("Transaction is already started")
	}

	t.maybeNewTransaction()
	t.status = TxOpen
	return nil
}
func (t *Transactional) maybeNewTransaction() {
	if t.stage == nil {
		t.stage = make(map[string]*stageDict)
	}
}

func (t *Transactional) Tx() []Op {
	if t.stage == nil {
		return nil
	}

	l := 0
	for _, dict := range t.stage {
		l += len(dict.ops)
	}

	ops := make([]Op, 0, l)
	for _, dict := range t.stage {
		for _, op := range dict.ops {
			ops = append(ops, op)
		}
	}
	return ops
}

func (t *Transactional) CommitTx() error {
	if t.status != TxOpen {
		return errors.New("No active transaction")
	}

	t.Commit()
	if len(t.stage) != 0 {
		t.stage = make(map[string]*stageDict)
	}
	return nil
}

func (t *Transactional) AbortTx() error {
	if t.status != TxOpen {
		return errors.New("No active transaction")
	}

	t.Abort()
	if len(t.stage) != 0 {
		t.stage = make(map[string]*stageDict)
	}
	return nil
}

func (t *Transactional) Save() ([]byte, error) {
	if t.status == TxOpen {
		return nil, ErrTxOpen
	}

	return t.State.Save()
}

func (t *Transactional) Restore(b []byte) error {
	return t.State.Restore(b)
}

func (t *Transactional) Dict(name string) Dict {
	if t.status != TxOpen {
		return t.State.Dict(name)
	}

	d := &stageDict{
		dict: t.State.Dict(name),
		ops:  make(map[string]Op),
	}

	t.stage[name] = d
	return d
}

func (t *Transactional) Commit() {
	for _, d := range t.stage {
		for _, o := range d.ops {
			switch o.T {
			case Put:
				d.dict.Put(o.K, o.V)
			case Del:
				d.dict.Del(o.K)
			}
		}
	}
	t.status = TxNone
}

func (t *Transactional) Abort() {
	t.status = TxNone
	return
}

func (d *stageDict) Name() string {
	return d.dict.Name()
}

func (d *stageDict) Put(k string, v []byte) error {
	d.ops[k] = Op{
		T: Put,
		D: d.dict.Name(),
		K: k,
		V: v,
	}
	return nil
}

func (d *stageDict) Get(k string) ([]byte, error) {
	op, ok := d.ops[k]
	if ok {
		switch op.T {
		case Put:
			return op.V, nil
		case Del:
			return nil, errors.New("No such key")
		}
	}

	return d.dict.Get(k)
}

func (d *stageDict) Del(k string) error {
	d.ops[k] = Op{
		T: Del,
		D: d.dict.Name(),
		K: k,
	}
	return nil
}

func (d *stageDict) ForEach(f IterFn) {
	d.dict.ForEach(func(k string, v []byte) {
		op, ok := d.ops[k]
		if ok {
			switch op.T {
			case Put:
				f(op.K, op.V)
				return
			case Del:
				return
			}
		}

		f(k, v)
	})
}

func (d *stageDict) GetGob(k string, v interface{}) error {
	return GetGob(d, k, v)
}

func (d *stageDict) PutGob(k string, v interface{}) error {
	return PutGob(d, k, v)
}
