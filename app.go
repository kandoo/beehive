package beehive

import (
	"errors"
	"fmt"

	"github.com/soheilhy/beehive/Godeps/_workspace/src/github.com/golang/glog"
	"github.com/soheilhy/beehive/state"
)

type AppFlag uint64

const (
	AppFlagSticky        AppFlag = 1 << iota
	AppFlagPersistent            = 1 << iota
	AppFlagTransactional         = 1 << iota
)

// Apps simply process and exchange messages. App methods are not
// thread-safe and we assume that neither are its map and receive functions.
type App interface {
	// Handles a specific message type using the handler. If msgType is an
	// name of msgType's reflection type.
	// instnace of MsgType, we use it as the type. Otherwise, we use the qualified
	Handle(msgType interface{}, h Handler) error
	// Hanldes a specific message type using the map and receive functions. If
	// msgType is an instnace of MsgType, we use it as the type. Otherwise, we use
	// the qualified name of msgType's reflection type.
	HandleFunc(msgType interface{}, m MapFunc, r RcvFunc) error

	// Regsiters the app's detached handler.
	Detached(h DetachedHandler)
	// Registers the detached handler using functions.
	DetachedFunc(start StartFunc, stop StopFunc, r RcvFunc)

	// Returns the state of this app that is shared among all instances and the
	// map function. This state is NOT thread-safe and apps must synchronize for
	// themselves.
	State() State
	// Returns the app name.
	Name() string

	// SetFlags sets the flags for this application.
	SetFlags(flag AppFlag)
	// Flags return the flags for this application.
	Flags() AppFlag

	// Whether the app is sticky.
	Sticky() bool
	// Whether to use the persistent state.
	Persistent() bool
	// Whether to use transactions.
	Transactional() bool

	// ReplicationFactor is the number of backup bees for the application. 1 means
	// no replication.
	ReplicationFactor() int
	// SetReplicationFactor sets the number of backup bees for this application.
	SetReplicationFactor(f int)

	// CommitThreshold is the minimum number of successful replications before
	// committing a replicated transaction.
	CommitThreshold() int

	// SetCommitThreshold sets the commit threshold.
	SetCommitThreshold(c int) error
}

// An applications map function that maps a specific message to the set of keys
// in state dictionaries. This method is assumed not to be thread-safe and is
// called sequentially. If the return value is an empty set the message is
// broadcasted to all local bees. Also, if the return value is nil, the message
// is drop.
type MapFunc func(m Msg, c MapContext) MappedCells

// An application recv function that handles a message. This method is called in
// parallel for different map-sets and sequentially within a map-set.
type RcvFunc func(m Msg, c RcvContext) error

// The interface msg handlers should implement.
type Handler interface {
	Map(m Msg, c MapContext) MappedCells
	Rcv(m Msg, c RcvContext) error
}

// Detached handlers, in contrast to normal Handlers with Map and Rcv, start in
// their own go-routine and emit messages. They do not listen on a particular
// message and only recv replys in their receive functions.
// Note that each app can have only one detached handler.
type DetachedHandler interface {
	// Starts the handler. Note that this will run in a separate goroutine, and
	// you can block.
	Start(ctx RcvContext)
	// Stops the handler. This should notify the start method perhaps using a
	// channel.
	Stop(ctx RcvContext)
	// Receives replies to messages emitted in this handler.
	Rcv(m Msg, ctx RcvContext) error
}

// Start function of a detached handler.
type StartFunc func(ctx RcvContext)

// Stop function of a detached handler.
type StopFunc func(ctx RcvContext)

type funcHandler struct {
	mapFunc  MapFunc
	recvFunc RcvFunc
}

func (h *funcHandler) Map(m Msg, c MapContext) MappedCells {
	return h.mapFunc(m, c)
}

func (h *funcHandler) Rcv(m Msg, c RcvContext) error {
	return h.recvFunc(m, c)
}

type funcDetached struct {
	startFunc StartFunc
	stopFunc  StopFunc
	recvFunc  RcvFunc
}

func (h *funcDetached) Start(c RcvContext) {
	h.startFunc(c)
}

func (h *funcDetached) Stop(c RcvContext) {
	h.stopFunc(c)
}

func (h *funcDetached) Rcv(m Msg, c RcvContext) error {
	return h.recvFunc(m, c)
}

type app struct {
	name         string
	hive         *hive
	qee          *qee
	handlers     map[string]Handler
	flags        AppFlag
	replFactor   int
	commitThresh int
}

func (a *app) HandleFunc(msg interface{}, m MapFunc, r RcvFunc) error {
	return a.Handle(msg, &funcHandler{m, r})
}

func (a *app) DetachedFunc(start StartFunc, stop StopFunc, rcv RcvFunc) {
	a.Detached(&funcDetached{start, stop, rcv})
}

func (a *app) Handle(msg interface{}, h Handler) error {
	if a.qee == nil {
		glog.Fatalf("App's qee is nil!")
	}

	t := msgType(msg)
	a.hive.RegisterMsg(msg)
	return a.registerHandler(t, h)
}

func (a *app) registerHandler(t string, h Handler) error {
	_, ok := a.handlers[t]
	a.handlers[t] = h
	a.hive.registerHandler(t, a.qee, h)

	if ok {
		return errors.New("A handler for this message type already exists.")
	}
	return nil
}

func (a *app) handler(t string) Handler {
	return a.handlers[t]
}

func (a *app) Detached(h DetachedHandler) {
	a.qee.ctrlCh <- newCmdAndChannel(cmdStartDetached{Handler: h}, a.Name(), 0,
		nil)
}

func (a *app) State() State {
	return a.qee.State()
}

func (a *app) Name() string {
	return a.name
}

func (a *app) SetFlags(flags AppFlag) {
	a.flags = flags
}

func (a *app) Flags() AppFlag {
	return a.flags
}

func (a *app) Sticky() bool {
	return a.flags&AppFlagSticky != 0
}

func (a *app) Persistent() bool {
	return a.flags&AppFlagPersistent != 0
}

func (a *app) Transactional() bool {
	return a.flags&AppFlagTransactional != 0
}

func (a *app) initQee() {
	// TODO(soheil): Maybe stop the previous qee if any?
	a.qee = &qee{
		dataCh: make(chan msgAndHandler, a.hive.config.DataChBufSize),
		ctrlCh: make(chan cmdAndChannel, a.hive.config.CmdChBufSize),
		hive:   a.hive,
		app:    a,
		bees:   make(map[uint64]bee),
	}
}

func (a *app) ReplicationFactor() int {
	return a.replFactor
}

func (a *app) SetReplicationFactor(f int) {
	a.SetFlags(a.flags | AppFlagTransactional | AppFlagPersistent)
	a.replFactor = f
	if a.commitThresh == 0 {
		a.commitThresh = f / 2
	}
}
func (a *app) CommitThreshold() int {
	return a.commitThresh
}

func (a *app) SetCommitThreshold(c int) error {
	if c > a.replFactor {
		a.commitThresh = a.replFactor - 1
		return fmt.Errorf("Commit threshold %d is lager than replication factor %d",
			c, a.replFactor)
	}

	a.commitThresh = c - 1
	return nil
}

func (a *app) newState() state.State {
	return state.NewInMem()
}
