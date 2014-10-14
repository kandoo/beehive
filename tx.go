package beehive

import (
	"fmt"

	"github.com/soheilhy/beehive/state"
)

// Tx represents the side effects of an operation: messages emitted during the
// transaction as well as state operations.
type Tx struct {
	state.Tx
	Msgs []Msg // Messages buffered in this tx.
}

// AddMsg adds a message to this transaction.
func (t *Tx) AddMsg(m Msg) {
	t.Msgs = append(t.Msgs, m)
}

// Reset simply clears messages and operations of the transaction.
func (t *Tx) Reset() {
	t.Tx.Reset()
	if !t.IsEmpty() {
		t.Msgs = nil
	}
}

// IsEmpty returns true if there is no messages or operations in the tx.
func (t *Tx) IsEmpty() bool {
	return len(t.Msgs) == 0 && len(t.Ops) == 0
}

func (t Tx) String() string {
	return fmt.Sprintf("Tx (msgs: %d, ops: %d, open: %v)",
		len(t.Msgs), len(t.Ops), t.Status == state.TxOpen)
}
