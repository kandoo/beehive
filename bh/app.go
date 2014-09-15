package bh

import (
	"errors"

	"github.com/golang/glog"
)

type AppName string

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
	HandleFunc(msgType interface{}, m Map, r Rcv) error

	// Regsiters the app's detached handler.
	Detached(h DetachedHandler)
	// Registers the detached handler using functions.
	DetachedFunc(start Start, stop Stop, r Rcv)

	// Returns the state of this app that is shared among all instances and the
	// map function. This state is NOT thread-safe and apps must synchronize for
	// themselves.
	State() State
	// Returns the app name.
	Name() AppName

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
}

// This is the list of dictionary keys returned by the map functions.
type MappedCells []CellKey

func (ms MappedCells) Len() int      { return len(ms) }
func (ms MappedCells) Swap(i, j int) { ms[i], ms[j] = ms[j], ms[i] }
func (ms MappedCells) Less(i, j int) bool {
	return ms[i].Dict < ms[j].Dict ||
		(ms[i].Dict == ms[j].Dict && ms[i].Key < ms[j].Key)
}

// An empty mapset means a local broadcast of message.
func (ms MappedCells) LocalBroadcast() bool {
	return len(ms) == 0
}

// An applications map function that maps a specific message to the set of keys
// in state dictionaries. This method is assumed not to be thread-safe and is
// called sequentially. If the return value is an empty set the message is
// broadcasted to all local bees. Also, if the return value is nil, the message
// is drop.
type Map func(m Msg, c MapContext) MappedCells

// An application recv function that handles a message. This method is called in
// parallel for different map-sets and sequentially within a map-set.
type Rcv func(m Msg, c RcvContext) error

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
type Start func(ctx RcvContext)

// Stop function of a detached handler.
type Stop func(ctx RcvContext)

type funcHandler struct {
	mapFunc  Map
	recvFunc Rcv
}

func (h *funcHandler) Map(m Msg, c MapContext) MappedCells {
	return h.mapFunc(m, c)
}

func (h *funcHandler) Rcv(m Msg, c RcvContext) error {
	return h.recvFunc(m, c)
}

type funcDetached struct {
	startFunc Start
	stopFunc  Stop
	recvFunc  Rcv
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
	name       AppName
	hive       *hive
	qee        *qee
	handlers   map[MsgType]Handler
	flags      AppFlag
	replFactor int
}

func (a *app) HandleFunc(msgType interface{}, m Map, r Rcv) error {
	return a.Handle(msgType, &funcHandler{m, r})
}

func (a *app) DetachedFunc(start Start, stop Stop, rcv Rcv) {
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

func (a *app) registerHandler(t MsgType, h Handler) error {
	_, ok := a.handlers[t]
	if ok {
		return errors.New("A handler for this message type already exists.")
	}

	a.handlers[t] = h
	a.hive.registerHandler(t, a.qee, h)
	return nil
}

func (a *app) handler(t MsgType) Handler {
	return a.handlers[t]
}

func (a *app) Detached(h DetachedHandler) {
	a.qee.ctrlCh <- NewLocalCmd(startDetachedCmd, h, BeeID{}, nil)
}

func (a *app) State() State {
	return a.qee.state()
}

func (a *app) Name() AppName {
	return a.name
}

func (a *app) SetFlags(flags AppFlag) {
	a.flags = flags
}

func (a *app) Flags() AppFlag {
	return a.flags
}

func (a *app) Sticky() bool {
	return a.flags&AppFlagSticky == AppFlagSticky
}

func (a *app) Persistent() bool {
	return a.flags&AppFlagPersistent == AppFlagPersistent
}

func (a *app) Transactional() bool {
	return a.flags&AppFlagTransactional == AppFlagTransactional
}

func (a *app) initQee() {
	// TODO(soheil): Maybe stop the previous qee if any?
	a.qee = &qee{
		dataCh: make(chan msgAndHandler, a.hive.config.DataChBufSize),
		ctrlCh: make(chan LocalCmd, a.hive.config.CmdChBufSize),
		ctx: mapContext{
			hive: a.hive,
			app:  a,
		},
		keyToBees: make(map[CellKey]bee),
		idToBees:  make(map[BeeID]bee),
	}
}

func (a *app) ReplicationFactor() int {
	return a.replFactor
}

func (a *app) SetReplicationFactor(f int) {
	a.replFactor = f
}
