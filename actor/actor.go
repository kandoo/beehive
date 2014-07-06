package actor

import (
	"encoding/gob"
	"errors"

	"github.com/golang/glog"
)

type ActorName string

// Actors simply process and exchange messages. Actor methods are not
// thread-safe and we assume that neither are its map and receive functions.
type Actor interface {
	// Handles a specific message type using the handler. If msgType is an
	// name of msgType's reflection type.
	// instnace of MsgType, we use it as the type. Otherwise, we use the qualified
	Handle(msgType interface{}, h Handler) error
	// Hanldes a specific message type using the map and receive functions. If
	// msgType is an instnace of MsgType, we use it as the type. Otherwise, we use
	// the qualified name of msgType's reflection type.
	HandleFunc(msgType interface{}, m Map, r Recv) error

	// Regsiters the actor's detached handler.
	Detached(h DetachedHandler) error
	// Registers the detached handler using functions.
	DetachedFunc(start Start, stop Stop, r Recv) error

	// Returns the state of this actor that is shared among all instances and the
	// map function. This state is NOT thread-safe and actors must synchronize for
	// themselves.
	State() State
	// Returns the actor name.
	Name() ActorName
}

// An applications map function that maps a specific message to the set of keys
// in state dictionaries. This method is assumed not to be thread-safe and is
// called sequentially.
type Map func(m Msg, c Context) MapSet

// An application recv function that handles a message. This method is called in
// parallel for different map-sets and sequentially within a map-set.
type Recv func(m Msg, c RecvContext)

// The interface msg handlers should implement.
type Handler interface {
	Map(m Msg, c Context) MapSet
	Recv(m Msg, c RecvContext)
}

// Detached handlers, in contrast to normal Handlers with Map and Recv, start in
// their own go-routine and emit messages. They do not listen on a particular
// message and only recv replys in their receive functions.
// Note that each actor can have only one detached handler.
type DetachedHandler interface {
	// Starts the handler. Note that this will run in a separate goroutine, and
	// you can block.
	Start(ctx RecvContext)
	// Stops the handler. This should notify the start method perhaps using a
	// channel.
	Stop(ctx RecvContext)
	// Receives replies to messages emitted in this handler.
	Recv(m Msg, ctx RecvContext)
}

// Start function of a detached handler.
type Start func(ctx RecvContext)

// Stop function of a detached handler.
type Stop func(ctx RecvContext)

type funcHandler struct {
	mapFunc  Map
	recvFunc Recv
}

func (h *funcHandler) Map(m Msg, c Context) MapSet {
	return h.mapFunc(m, c)
}

func (h *funcHandler) Recv(m Msg, c RecvContext) {
	h.recvFunc(m, c)
}

type funcDetached struct {
	startFunc Start
	stopFunc  Stop
	recvFunc  Recv
}

func (h *funcDetached) Start(c RecvContext) {
	h.startFunc(c)
}

func (h *funcDetached) Stop(c RecvContext) {
	h.stopFunc(c)
}

func (h *funcDetached) Recv(m Msg, c RecvContext) {
	h.recvFunc(m, c)
}

type actor struct {
	name     ActorName
	stage    *stage
	mapper   *mapper
	handlers map[MsgType]Handler
}

func (a *actor) HandleFunc(msgType interface{}, m Map, r Recv) error {
	return a.Handle(msgType, &funcHandler{m, r})
}

func (a *actor) DetachedFunc(start Start, stop Stop, rcv Recv) error {
	return a.Detached(&funcDetached{start, stop, rcv})
}

func (a *actor) Handle(msgT interface{}, h Handler) error {
	if a.mapper == nil {
		glog.Fatalf("Actor's mapper is nil!")
	}

	t, ok := msgT.(MsgType)
	if !ok {
		t = msgType(msgT)
		gob.Register(msgT)
	}

	err := a.registerHandler(t, h)
	if err != nil {
		return err
	}

	a.stage.registerHandler(t, a.mapper, h)
	return nil
}

func (a *actor) registerHandler(t MsgType, h Handler) error {
	_, ok := a.handlers[t]
	if ok {
		return errors.New("A handler for this message type already exists.")
	}

	a.handlers[t] = h
	return nil
}

func (a *actor) handler(t MsgType) Handler {
	return a.handlers[t]
}

func (a *actor) Detached(h DetachedHandler) error {
	return a.mapper.registerDetached(h)
}

func (a *actor) State() State {
	return a.mapper.state()
}

func (a *actor) Name() ActorName {
	return a.name
}

func (a *actor) initMapper() {
	// TODO(soheil): Maybe stop the previous mapper if any?
	a.mapper = &mapper{
		asyncRoutine: asyncRoutine{
			dataCh: make(chan msgAndHandler, a.stage.config.DataChBufSize),
			ctrlCh: make(chan routineCmd),
			waitCh: make(chan interface{}),
		},
		ctx: context{
			stage: a.stage,
			actor: a,
		},
		keyToRcvrs: make(map[string]receiver),
		idToRcvrs:  make(map[RcvrId]receiver),
	}

	go a.mapper.start()
}
