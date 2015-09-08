package beehive

import (
	"encoding/gob"
	"time"

	"github.com/kandoo/beehive/Godeps/_workspace/src/golang.org/x/net/context"
	"github.com/kandoo/beehive/state"
)

// Context is the interface shared between MapContext and RcvContext. It wraps
// Hive(), App() and Dict().
type Context interface {
	// Hive returns the Hive of this context.
	Hive() Hive
	// App returns the name of the application of this context.
	App() string
	// Dict is a helper function that returns the specific dict within the state.
	Dict(name string) state.Dict
	// Sync processes a synchrounous message (req) and blocks until the response
	// is recieved.
	Sync(ctx context.Context, req interface{}) (res interface{}, err error)
	// Printf formats according to format string and writes the string on
	// standard output.
	//
	// Note: This method is solely for debugging your message handlers.
	// For proper logging, use glog.
	Printf(format string, a ...interface{})
}

// MapContext is passed to the map functions of message handlers. It provides
// all the platform-level functions required to implement the map function.
type MapContext interface {
	Context

	// LocalMappedCells returns a mapped cell unique to the hive of this map
	// context.
	LocalMappedCells() MappedCells
}

// Repliable is a serializable structure that can be used to reply to a message
// at any time. Repliable is always created using RcvContext.DeferReply().
//
// Note: The fields in the Repliable are public for serialization. It is not
// advisable to modify these fields.
type Repliable struct {
	From   uint64 // The ID of the bee that originally sent the message.
	SyncID uint64 // The sync message ID if the message was sync, otherwise 0.
}

// Reply replies to the Repliable using replyData.
func (r *Repliable) Reply(ctx RcvContext, replyData interface{}) {
	if r.SyncID != 0 {
		replyData = syncRes{
			ID:   r.SyncID,
			Data: replyData,
		}
	}
	ctx.SendToBee(replyData, r.From)
}

// RcvContext is passed to the rcv functions of message handlers. It provides
// all the platform-level functions required to implement the rcv function.
type RcvContext interface {
	Context

	// ID returns the bee id of this context.
	ID() uint64

	// Emit emits a message.
	Emit(msgData interface{})
	// SendToCell sends a message to the bee of the give app that owns the
	// given cell.
	SendToCell(msgData interface{}, app string, cell CellKey)
	// SendToBee sends a message to the given bee.
	SendToBee(msgData interface{}, to uint64)
	// Reply replies to a message: Sends a message from the current bee to the
	// bee that emitted msg.
	Reply(msg Msg, replyData interface{}) error
	// DeferReply returns a Repliable that can be used to reply to a
	// message (either a sync or a async message) later.
	DeferReply(msg Msg) Repliable

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

func init() {
	gob.Register(Repliable{})
}
