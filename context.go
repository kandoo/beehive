package beehive

import (
	"time"

	"github.com/kandoo/beehive/state"
)

type Context interface {
	// Hive returns the Hive of this context.
	Hive() Hive
	// App returns the name of the application of this context.
	App() string
	// Dict is a helper function that returns the specific dict within the state.
	Dict(name string) state.Dict
}

// MapContext is passed to the map functions of message handlers. It provides
// all the platform-level functions required to implement the map function.
type MapContext interface {
	Context

	// LocalMappedCells returns a mapped cell unique to the hive of this map
	// context.
	LocalMappedCells() MappedCells
}

// RcvContext is passed to the rcv functions of message handlers. It provides
// all the platform-level functions required to implement the rcv function.
type RcvContext interface {
	Context

	// ID returns the bee id of this context.
	ID() uint64

	// Emit emits a message.
	Emit(msgData interface{})
	// SendToCellKey sends a message to the bee of the give app that owns the
	// given cell key.
	SendToCellKey(msgData interface{}, to string, dk CellKey)
	// SendToBee sends a message to the given bee.
	SendToBee(msgData interface{}, to uint64)
	// ReplyTo replies to a message: Sends a message from the current bee to the
	// bee that emitted msg.
	ReplyTo(msg Msg, replyData interface{}) error

	// StartDetached spawns a detached handler.
	StartDetached(h DetachedHandler) uint64
	// StartDetachedFunc spawns a detached handler using the provide function.
	StartDetachedFunc(start StartFunc, stop StopFunc, rcv RcvFunc) uint64

	// LockCells proactively locks the cells in the given cell keys.
	LockCells(keys []CellKey) error

	// Snooze exits the Rcv function, and schedules the current message to be
	// enqued again after at least duration d.
	Snooze(d time.Duration)

	// BeeLocal returns the bee-local storage. It is an ephemeral memory that is
	// just visible to the current bee. Very similar to thread-locals in the scope
	// of a bee.
	BeeLocal() interface{}
	// SetBeeLocal sets a data in the bee-local storage.
	SetBeeLocal(d interface{})

	// Starts a transaction in this context. Transactions span multiple
	// dictionaries and buffer all messages. When a transaction commits all the
	// side effects will be applied. Note that since handlers are called in a
	// single bee, transactions are mostly for programming convinience and easy
	// atomocity.
	BeginTx() error
	// Commits the current transaction.
	// If the application has a 2+ replication factor, calling commit also means
	// that we will wait until the transaction is sufficiently replicated and then
	// commits the transaction.
	CommitTx() error
	// Aborts the transaction.
	AbortTx() error
}
