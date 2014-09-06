package bh

import (
	"errors"

	"github.com/golang/glog"
)

type AppName string

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

	// Whether the app is sticky.
	Sticky() bool
	// Sets whether the app is sticky, i.e., should not be migrated.
	SetSticky(sticky bool)

	// Whether to use PersistentState.
	PersistentState() bool
	// Whether to use persistent state.
	SetPersistentState(p bool)
}

// This is the list of dictionary keys returned by the map functions.
type MapSet []DictionaryKey

func (ms MapSet) Len() int      { return len(ms) }
func (ms MapSet) Swap(i, j int) { ms[i], ms[j] = ms[j], ms[i] }
func (ms MapSet) Less(i, j int) bool {
	return ms[i].Dict < ms[j].Dict ||
		(ms[i].Dict == ms[j].Dict && ms[i].Key < ms[j].Key)
}

// An empty mapset means a local broadcast of message.
func (ms MapSet) LocalBroadcast() bool {
	return len(ms) == 0
}

// An applications map function that maps a specific message to the set of keys
// in state dictionaries. This method is assumed not to be thread-safe and is
// called sequentially. If the return value is an empty set the message is
// broadcasted to all local bees. Also, if the return value is nil, the message
// is drop.
type Map func(m Msg, c MapContext) MapSet

// An application recv function that handles a message. This method is called in
// parallel for different map-sets and sequentially within a map-set.
type Rcv func(m Msg, c RcvContext) error

// The interface msg handlers should implement.
type Handler interface {
	Map(m Msg, c MapContext) MapSet
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

func (h *funcHandler) Map(m Msg, c MapContext) MapSet {
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
	sticky     bool
	persistent bool
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
	a.qee.registerDetached(h)
}

func (a *app) State() State {
	return a.qee.state()
}

func (a *app) Name() AppName {
	return a.name
}

func (a *app) SetSticky(sticky bool) {
	a.sticky = sticky
}

func (a *app) Sticky() bool {
	return a.sticky
}

func (a *app) PersistentState() bool {
	return a.persistent
}

func (a *app) SetPersistentState(p bool) {
	a.persistent = p
}

func (a *app) initQee() {
	// TODO(soheil): Maybe stop the previous qee if any?
	a.qee = &qee{
		asyncRoutine: asyncRoutine{
			dataCh: make(chan msgAndHandler, a.hive.config.DataChBufSize),
			ctrlCh: make(chan routineCmd),
		},
		ctx: mapContext{
			hive: a.hive,
			app:  a,
		},
		keyToBees: make(map[DictionaryKey]bee),
		idToBees:  make(map[BeeId]bee),
	}
}
