package beehive

import "github.com/soheilhy/beehive/state"

// State is the storage for a collection of dictionaries.
type State interface {
	// Returns a dictionary for this state. Creates one if it does not exist.
	Dict(name string) state.Dict
	// Save save the state into bytes.
	Save() ([]byte, error)
	// Restore restores the state from b.
	Restore(b []byte) error
	// Starts a transaction for this state. Transactions span multiple
	// dictionaries.
	BeginTx() error
	// Commits the current transaction.
	CommitTx() error
	// Aborts the transaction.
	AbortTx() error
	// TxStatus returns the transaction status.
	TxStatus() state.TxStatus
	// Tx returns all the operations in the transaction.
	Tx() []state.Op
}
