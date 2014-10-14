package state

import (
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
