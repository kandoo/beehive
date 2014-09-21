package bh

import "fmt"

// TxGeneration is the generation of the colony that began the transaction.
type TxGeneration uint64

// TxSeq represents a monotonically increasing number (with the possbility of a
// wrap around) for the transaction.
type TxSeq uint64

// TxStatus represents the status of a transaction.
type TxStatus uint8

// Valid values for TxStatus
const (
	TxCommitted TxStatus = iota
	TxOpen               = iota
)

// Tx represents the side effects of an operation: messages emitted during the
// transaction as well as state operations.
type Tx struct {
	Generation TxGeneration // Generation of the colony that began this tx.
	Seq        TxSeq        // An motonically increasing sequence of the tx.
	Msgs       []Msg        // Messages buffered in this tx.
	Ops        []StateOp    // State operations in this tx.
	Status     TxStatus     // Status of this tx.
}

// AddMsg adds a message to this transaction.
func (t *Tx) AddMsg(msg Msg) {
	t.Msgs = append(t.Msgs, msg)
}

// AddOp adds a state operation to this transaction.
func (t *Tx) AddOp(op StateOp) {
	t.Ops = append(t.Ops, op)
}

// IsOpen returns true if the transaction is open.
func (t *Tx) IsOpen() bool {
	return t.Status == TxOpen
}

// Reset simply clears messages and operations of the transaction. It does not
// change the sequence nor the generation.
func (t *Tx) Reset() {
	t.Msgs = nil
	t.Ops = nil
	t.Status = TxCommitted
}

// IsEmpty returns true if there is no messages or operations in the tx.
func (t *Tx) IsEmpty() bool {
	return len(t.Msgs) == 0 && len(t.Ops) == 0
}

func (t Tx) String() string {
	return fmt.Sprintf("Tx %d/%d (msgs: %d, ops: %d, open: %v)",
		t.Generation, t.Seq, len(t.Msgs), len(t.Ops), t.Status == TxOpen)
}

type TxInfo struct {
	Generation    TxGeneration
	LastCommitted TxSeq
	LastBuffered  TxSeq
}

func (i TxInfo) String() string {
	return fmt.Sprintf("TxInfo (gen=%v, buf=%v, committed=%v)", i.Generation,
		i.LastBuffered, i.LastCommitted)
}
