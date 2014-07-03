package actor

import "github.com/golang/glog"

type ActorName string

// Actors simply process and exchange messages. Actor methods are not
// thread-safe and we assume that neither are its map and receive functions.
type Actor interface {
	// Hanldes a specific message type using the map and receive functions. If
	// msgType is an instnace of MsgType, we use it as the type. Otherwise, we use
	// the qualified name of msgType's reflection type.
	HandleFunc(msgType interface{}, m Map, r Recv)
	// Handles a specific message type using the handler. If msgType is an
	// instnace of MsgType, we use it as the type. Otherwise, we use the qualified
	// name of msgType's reflection type.
	Handle(msgType interface{}, h Handler)
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

// Creates a handler using the given map and recv functions.
func handlerFromFuncs(m Map, r Recv) Handler {
	return &funcHandler{m, r}
}

type actor struct {
	name   ActorName
	stage  *stage
	mapper *mapper
}

func (a *actor) HandleFunc(msgType interface{}, m Map, r Recv) {
	a.Handle(msgType, handlerFromFuncs(m, r))
}

func (a *actor) Handle(msgT interface{}, h Handler) {
	t, ok := msgT.(MsgType)
	if !ok {
		t = msgType(msgT)
	}

	if a.mapper == nil {
		glog.Fatalf("Actor's mapper is nil!")
	}

	a.stage.registerHandler(t, a.mapper, h)
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
			ctrlCh: make(chan routineCommand),
			waitCh: make(chan interface{}),
		},
		ctx: context{
			stage: a.stage,
			actor: a,
		},
		receivers: make(map[string]receiver),
	}

	go a.mapper.start()
}
