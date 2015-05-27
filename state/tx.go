package state

import (
	"errors"
	"fmt"

	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/golang/glog"
)

// TxStatus represents the status of a transaction.
type TxStatus uint8

// Valid values for TxStatus
const (
	TxNone TxStatus = iota
	TxOpen          = iota
)

var (
	ErrOpenTx error = errors.New("tx: transaction is already open")
	ErrNoTx   error = errors.New("tx: no open transaction")
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
	State
	stage  map[string]*TxDict
	status TxStatus
}

func (t *Transactional) TxStatus() TxStatus {
	return t.status
}

func (t *Transactional) BeginTx() error {
	if t.status == TxOpen {
		return ErrOpenTx
	}

	t.maybeNewTransaction()
	t.status = TxOpen
	return nil
}
func (t *Transactional) maybeNewTransaction() {
	if t.stage == nil {
		t.stage = make(map[string]*TxDict)
	}
}

func (t *Transactional) Tx() Tx {
	return Tx{
		Status: t.TxStatus(),
		Ops:    t.TxOps(),
	}
}

func (t *Transactional) TxOps() []Op {
	if t.stage == nil {
		return nil
	}

	l := 0
	for _, dict := range t.stage {
		l += len(dict.Ops)
	}

	ops := make([]Op, 0, l)
	for _, dict := range t.stage {
		for _, op := range dict.Ops {
			ops = append(ops, op)
		}
	}
	return ops
}

func (t *Transactional) CommitTx() error {
	if t.status != TxOpen {
		return ErrNoTx
	}

	for _, d := range t.stage {
		d.CommitTx()
	}
	t.Reset()
	return nil
}

func (t *Transactional) AbortTx() error {
	if t.status != TxOpen {
		return ErrNoTx
	}

	t.Reset()
	return nil
}

func (t *Transactional) Reset() {
	t.status = TxNone
	if len(t.stage) == 0 {
		return
	}
	for _, d := range t.stage {
		d.reset()
	}
}

func (t *Transactional) HasEmptyTx() bool {
	return len(t.stage) == 0
}

func (t *Transactional) Apply(ops []Op) error {
	if t.status == TxOpen {
		return ErrOpenTx
	}
	for _, o := range ops {
		switch o.T {
		case Put:
			t.Dict(o.D).Put(o.K, o.V)
		case Del:
			t.Dict(o.D).Del(o.K)
		}
	}
	return nil
}

func (t *Transactional) Save() ([]byte, error) {
	if t.status == TxOpen {
		glog.Warningf("transactional has an open tx when the snapshot is taken")
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

	d, ok := t.stage[name]
	if ok {
		d.Status = TxOpen
		return d
	}

	d = &TxDict{
		Dict: t.State.Dict(name),
		Ops:  make(map[string]Op),
	}
	d.BeginTx()
	t.stage[name] = d
	return d
}

// TxDict implements the Dict interface, and wraps any dictionary and make it
// transactional. All modifications will fail if there is no open tx.
type TxDict struct {
	Dict   Dict
	Status TxStatus
	Ops    map[string]Op
}

func (d *TxDict) Name() string {
	return d.Dict.Name()
}

func (d *TxDict) Put(k string, v []byte) error {
	d.Ops[k] = Op{
		T: Put,
		D: d.Dict.Name(),
		K: k,
		V: v,
	}
	return nil
}

func (d *TxDict) Get(k string) ([]byte, error) {
	op, ok := d.Ops[k]
	if ok {
		switch op.T {
		case Put:
			return op.V, nil
		case Del:
			return nil, errors.New("No such key")
		}
	}
	return d.Dict.Get(k)
}

func (d *TxDict) Del(k string) error {
	d.Ops[k] = Op{
		T: Del,
		D: d.Dict.Name(),
		K: k,
	}
	return nil
}

func (d *TxDict) ForEach(f IterFn) {
	d.Dict.ForEach(func(k string, v []byte) {
		op, ok := d.Ops[k]
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

func (d *TxDict) GetGob(k string, v interface{}) error {
	return GetGob(d, k, v)
}

func (d *TxDict) PutGob(k string, v interface{}) error {
	return PutGob(d, k, v)
}

func (d *TxDict) BeginTx() error {
	if d.Status == TxOpen {
		return ErrOpenTx
	}
	d.Status = TxOpen
	return nil
}

func (d *TxDict) CommitTx() error {
	if d.Status == TxNone {
		return ErrNoTx
	}
	for _, o := range d.Ops {
		switch o.T {
		case Put:
			d.Dict.Put(o.K, o.V)
		case Del:
			d.Dict.Del(o.K)
		}
	}
	d.reset()
	return nil
}

func (d *TxDict) AbortTx() error {
	if d.Status == TxNone {
		return ErrNoTx
	}
	d.reset()
	return nil
}

func (d *TxDict) reset() {
	d.Status = TxNone
	if len(d.Ops) == 0 {
		return
	}
	// if there are more than 10 ops, re-create the map.
	if len(d.Ops) > 10 {
		d.Ops = make(map[string]Op)
		return
	}

	for k := range d.Ops {
		delete(d.Ops, k)
	}
}
