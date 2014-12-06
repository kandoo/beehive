package beehive

import (
	"encoding/gob"
	"fmt"

	"github.com/kandoo/beehive/state"
)

// tx represents the side effects of an operation: messages emitted during the
// transaction as well as state operations.
type tx struct {
	state.Tx
	Msgs []*msg // Messages buffered in this tx.
}

// addMsg adds a message to this transaction.
func (t *tx) addMsg(m *msg) {
	t.Msgs = append(t.Msgs, m)
}

// reset simply clears messages and operations of the transaction.
func (t *tx) reset() {
	t.Tx.Reset()
	if !t.IsEmpty() {
		t.Msgs = nil
	}
}

// empty returns true if there is no messages or operations in the tx.
func (t *tx) empty() bool {
	return len(t.Msgs) == 0 && len(t.Ops) == 0
}

func (t tx) String() string {
	return fmt.Sprintf("tx (msgs: %d, ops: %d, open: %v)",
		len(t.Msgs), len(t.Ops), t.Status == state.TxOpen)
}

func init() {
	gob.Register(tx{})
}
