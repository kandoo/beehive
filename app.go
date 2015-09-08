package beehive

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/golang/glog"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/gorilla/mux"
	"github.com/kandoo/beehive/bucket"
	"github.com/kandoo/beehive/state"
)

// App represents an application in beehive. An app is a collection of stateful
// message handlers.
//
// Methods in this interface are not thread-safe and must be called before the
// Hive starts.
type App interface {
	// Returns the app name.
	Name() string

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

	// Returns the state of this app that is used in the map function. This state
	// is NOT thread-safe and apps must synchronize for themselves.
	Dict(name string) state.Dict

	// HandleHTTP registers an HTTP handler for this application on
	// "/apps/name/path".
	//
	// Note: Gorilla mux is used internally. As such, it is legal to use path
	// parameters.
	HandleHTTP(path string, handler http.Handler) *mux.Route
	// HandleHTTPFunc registers an HTTP handler func for this application on
	// "/app/name/path".
	//
	// Note: Gorilla mux is used internally. As such, it is legal to use path
	// parameters.
	HandleHTTPFunc(path string,
		handler func(http.ResponseWriter, *http.Request)) *mux.Route
}

// AppOption represents an option for applications.
type AppOption func(a *app)

// Persistent is an application option that makes the application's state
// persistent. The app state will be replciated on "replicationFactor" hives.
// This option also makes the application transactional.
func Persistent(replicationFactor int) AppOption {
	return func(a *app) {
		a.flags = a.flags | appFlagPersistent | appFlagTransactional
		a.replFactor = replicationFactor
	}
}

// Transactional is an application option that makes the application
// transactional. Transactions embody both application messages and its state.
func Transactional() AppOption {
	return func(a *app) {
		a.flags |= appFlagTransactional
	}
}

// Sticky is an application option that makes the application sticky. Bees of
// sticky apps are not migrated by the optimizer.
func Sticky() AppOption {
	return func(a *app) {
		a.flags |= appFlagSticky
	}
}

// NonTransactional is an application option that makes the application
// non-transactional.
func NonTransactional() AppOption {
	return func(a *app) {
		a.flags &= ^appFlagTransactional
	}
}

// Placement is an application option that customizes the default
// placement strategy for the application.
func Placement(m PlacementMethod) AppOption {
	return func(a *app) {
		a.placement = m
	}
}

// InRate is an application option that limits the rate of incoming messages of
// each bee of an application using a token bucket with the given rate and the
// given maximum.
func InRate(rate bucket.Rate, max uint64) AppOption {
	return func(a *app) {
		a.rate.inRate = rate
		a.rate.inMaxTokens = max
	}
}

// OutRate is an application option that limits the rate of outgoing messages of
// each bee of an application using a token bucket with the given rate and the
// given maximum.
func OutRate(rate bucket.Rate, max uint64) AppOption {
	return func(a *app) {
		a.rate.outRate = rate
		a.rate.outMaxTokens = max
	}
}

// MapFunc is a map function that maps a specific message to the set of keys
// in state dictionaries. This method is assumed not to be thread-safe and is
// called sequentially. If the return value is an empty set the message is
// broadcasted to all local bees. Also, if the return value is nil, the message
// is drop.
type MapFunc func(m Msg, c MapContext) MappedCells

// RcvFunc is the function that handles a message. This method is called in
// parallel for different map-sets and sequentially within a map-set.
type RcvFunc func(m Msg, c RcvContext) error

// Receiver wraps Rcv.
type Receiver interface {
	// Receives replies to messages emitted in this handler.
	Rcv(m Msg, c RcvContext) error
}

// Handler represents a message handler.
type Handler interface {
	Receiver
	Map(m Msg, c MapContext) MappedCells
}

// DetachedHandler in contrast to normal Handlers with Map and Rcv, starts in
// their own go-routine and emit messages. They do not listen on a particular
// message and only recv replys in their receive functions.
// Note that each app can have only one detached handler.
type DetachedHandler interface {
	Receiver
	// Starts the handler. Note that this will run in a separate goroutine, and
	// you can block.
	Start(ctx RcvContext)
	// Stops the handler. This should notify the start method perhaps using a
	// channel.
	Stop(ctx RcvContext)
}

// StartFunc is the start function of a detached handler.
type StartFunc func(ctx RcvContext)

// StopFunc is the stop function of a detached handler.
type StopFunc func(ctx RcvContext)

type funcHandler struct {
	mapFunc MapFunc
	rcvFunc RcvFunc
}

func (h *funcHandler) Map(m Msg, c MapContext) MappedCells {
	return h.mapFunc(m, c)
}

func (h *funcHandler) Rcv(m Msg, c RcvContext) error {
	return h.rcvFunc(m, c)
}

type funcDetached struct {
	startFunc StartFunc
	stopFunc  StopFunc
	rcvFunc   RcvFunc
}

func (h *funcDetached) Start(c RcvContext) {
	h.startFunc(c)
}

func (h *funcDetached) Stop(c RcvContext) {
	h.stopFunc(c)
}

func (h *funcDetached) Rcv(m Msg, c RcvContext) error {
	return h.rcvFunc(m, c)
}

type runtimeRcvContext struct {
	*qee
	state *state.Transactional
}

func (c runtimeRcvContext) ID() uint64 {
	return 0
}

func (c runtimeRcvContext) Printf(format string, a ...interface{}) {}

func (c runtimeRcvContext) Emit(msgData interface{}) {}

func (c runtimeRcvContext) SendToCell(msgData interface{}, app string,
	cell CellKey) {
}

func (c runtimeRcvContext) SendToBee(msgData interface{}, to uint64) {}

func (c runtimeRcvContext) Reply(msg Msg, replyData interface{}) error {
	return nil
}

func (c runtimeRcvContext) DeferReply(msg Msg) Repliable {
	return Repliable{}
}

func (c runtimeRcvContext) StartDetached(h DetachedHandler) uint64 {
	return 0
}

func (c runtimeRcvContext) StartDetachedFunc(start StartFunc, stop StopFunc,
	rcv RcvFunc) uint64 {

	return 0
}

func (c runtimeRcvContext) LockCells(keys []CellKey) error {
	return nil
}

func (c runtimeRcvContext) Snooze(d time.Duration) {}

func (c runtimeRcvContext) BeeLocal() interface{} {
	return nil
}

func (c runtimeRcvContext) SetBeeLocal(d interface{}) {}

func (c runtimeRcvContext) Dict(name string) state.Dict {
	return c.state.Dict(name)
}

func (c runtimeRcvContext) BeginTx() error {
	return c.state.BeginTx()
}

func (c runtimeRcvContext) AbortTx() error {
	return c.state.AbortTx()
}

func (c runtimeRcvContext) CommitTx() error {
	return c.state.CommitTx()
}

// RuntimeMap generates an automatic runtime map function based on the given
// rcv function.
//
// If there was an error in the rcv function, it will return "nil" and the
// message will be dropped.
func RuntimeMap(rcv RcvFunc) MapFunc {
	return func(msg Msg, ctx MapContext) (cells MappedCells) {
		defer func() {
			if r := recover(); r != nil {
				glog.Errorf("runtime map cannot find the mapped cells")
				cells = nil
			}
		}()

		q := ctx.(*qee)
		rCtx := runtimeRcvContext{
			qee:   q,
			state: state.NewTransactional(q.app.newState()),
		}

		if err := rcv(msg, rCtx); err != nil {
			return nil
		}

		for _, d := range rCtx.state.Dicts() {
			d.ForEach(func(k string, v interface{}) bool {
				cells = append(cells, CellKey{Dict: d.Name(), Key: k})
				return true
			})
		}

		return
	}
}

var (
	defaultAppOptions = []AppOption{Transactional()}
)

type appFlag uint64

const (
	appFlagSticky appFlag = 1 << iota
	appFlagPersistent
	appFlagTransactional
)

type appRate struct {
	inRate       bucket.Rate
	inMaxTokens  uint64
	outRate      bucket.Rate
	outMaxTokens uint64
}

type app struct {
	name       string
	hive       *hive
	qee        *qee
	handlers   map[string]Handler
	flags      appFlag
	replFactor int
	placement  PlacementMethod
	router     *mux.Router
	rate       appRate
}

func (a *app) String() string {
	return fmt.Sprintf("%v/%s", a.hive, a.name)
}

func (a *app) HandleFunc(msg interface{}, m MapFunc, r RcvFunc) error {
	return a.Handle(msg, &funcHandler{m, r})
}

func (a *app) DetachedFunc(start StartFunc, stop StopFunc, rcv RcvFunc) {
	a.Detached(&funcDetached{start, stop, rcv})
}

func (a *app) Handle(msg interface{}, h Handler) error {
	if a.qee == nil {
		glog.Fatalf("app's qee is nil!")
	}

	t := MsgType(msg)
	a.hive.RegisterMsg(msg)
	if err := a.registerHandler(t, h); err != nil {
		return err
	}

	s := syncReq{Data: msg}
	t = MsgType(s)
	return a.registerHandler(t, syncHandler{handler: h})
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
	a.qee.ctrlCh <- newCmdAndChannel(cmdStartDetached{Handler: h}, a.hive.ID(),
		a.Name(), 0, nil)
}

func (a *app) Dict(name string) state.Dict {
	return a.qee.Dict(name)
}

func (a *app) Name() string {
	return a.name
}

func (a *app) HandleHTTP(path string, handler http.Handler) *mux.Route {
	return a.subrouter().Handle(path, handler)
}

func (a *app) HandleHTTPFunc(path string,
	handler func(http.ResponseWriter, *http.Request)) *mux.Route {
	return a.subrouter().HandleFunc(path, handler)
}

func (a *app) subrouter() *mux.Router {
	if a.router == nil {
		a.router = a.hive.httpServer.router.PathPrefix(a.appHTTPPrefix()).Subrouter()
	}
	return a.router
}

func (a *app) appHTTPPrefix() string {
	return "/apps/" + a.name + "/"
}

func (a *app) initQee() {
	a.qee = &qee{
		dataCh:       newMsgChannel(a.hive.config.DataChBufSize),
		ctrlCh:       make(chan cmdAndChannel, a.hive.config.CmdChBufSize),
		placementCh:  make(chan placementRes, a.hive.config.CmdChBufSize),
		hive:         a.hive,
		app:          a,
		bees:         make(map[uint64]*bee),
		state:        state.NewTransactional(a.newState()),
		pendingCells: make(map[CellKey]*pendingCells),
	}
}

func (a *app) newState() state.State {
	return state.NewInMem()
}

func (a *app) persistent() bool {
	return a.flags&appFlagPersistent != 0
}

func (a *app) transactional() bool {
	return a.flags&appFlagTransactional != 0
}

func (a *app) sticky() bool {
	return a.flags&appFlagSticky != 0
}
