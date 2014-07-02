package actor

import (
	"fmt"

	"github.com/golang/glog"
)

type ActorName string

type mapperAndHandler struct {
	mapr    *mapper
	handler Handler
}

type ReceiverId struct {
	StageId   StageId
	ActorName ActorName
	RcvrId    uint32
}

type Context interface {
	State() State
}

type RecvContext interface {
	Context
	Emit(msgData interface{})
	SendTo(msgData interface{}, to ActorName, dk DictionaryKey)
	ReplyTo(msg Msg, replyData interface{})
}

type context struct {
	state State
	stage *stage
	actor *actor
}

type recvContext struct {
	context
	rcvr receiver
}

func (ctx *context) State() State {
	if ctx.state == nil {
		ctx.state = newState(string(ctx.actor.Name()))
	}

	return ctx.state
}

// Emits a message. Note that m should be your data not an instance of Msg.
func (ctx *recvContext) Emit(msgData interface{}) {
	msg := broadcastMsg{
		simpleMsg: simpleMsg{
			stage:   ctx.stage,
			data:    msgData,
			msgType: msgType(msgData),
		},
		from: ctx.rcvr.id(),
	}

	ctx.stage.EmitMsg(&msg)
}

func (ctx *recvContext) SendTo(msgData interface{}, to ActorName,
	dk DictionaryKey) {

	msg := unicastMsg{
		broadcastMsg: broadcastMsg{
			simpleMsg: simpleMsg{
				stage:   ctx.stage,
				data:    msgData,
				msgType: msgType(msgData),
			},
			from: ctx.rcvr.id(),
		},
	}

	ctx.stage.EmitMsg(&msg)

	// TODO(soheil): Implement send to.
	glog.Fatal("Sendto is not implemented.")
}

func (ctx *recvContext) ReplyTo(msg Msg, replyData interface{}) {
	// TODO(soheil): Implement reply to.
	glog.Fatal("RelpyTo is not implemented.")
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

func newState(name string) State {
	return &inMemoryState{name, make(map[string]Dictionary)}
}

type inMemoryState struct {
	name  string
	dicts map[string]Dictionary
}

type inMemoryDictionary struct {
	name DictionaryName
	dict map[Key]Value
}

func (d *inMemoryDictionary) Get(k Key) (Value, bool) {
	v, ok := d.dict[k]
	return v, ok
}

func (d *inMemoryDictionary) Set(k Key, v Value) {
	d.dict[k] = v
}

func (d *inMemoryDictionary) Name() DictionaryName {
	return d.name
}

func (s *inMemoryState) Dict(name DictionaryName) Dictionary {
	d, ok := s.dicts[string(name)]
	if !ok {
		d = &inMemoryDictionary{name, make(map[Key]Value)}
		s.dicts[string(name)] = d
	}
	return d
}

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
			ctrlCh: make(chan actorCommand),
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

type actorCommand int

const (
	stopActor  actorCommand = iota
	startActor              = iota
)

type msgAndHandler struct {
	msg     Msg
	handler Handler
}

type asyncRoutine struct {
	dataCh chan msgAndHandler
	ctrlCh chan actorCommand
	waitCh chan interface{}
}

type mapper struct {
	asyncRoutine
	ctx       context
	receivers map[string]receiver
	lastRId   uint32
}

func (mapr *mapper) state() State {
	if mapr.ctx.state == nil {
		mapr.ctx.state = newState(string(mapr.ctx.actor.Name()))
	}
	return mapr.ctx.state
}

type receiver interface {
	id() ReceiverId
	enque(mh msgAndHandler)
	start()
}

type localRcvr struct {
	asyncRoutine
	ctx recvContext
	rId ReceiverId
}

func (rcvr *localRcvr) id() ReceiverId {
	return rcvr.rId
}

func (rcvr *localRcvr) start() {
	for {
		select {
		case d, ok := <-rcvr.dataCh:
			if !ok {
				return
			}
			rcvr.handleMsg(d)

		case c, ok := <-rcvr.ctrlCh:
			if !ok {
				return
			}
			rcvr.handleCmd(c)
		}
	}
}

func (rcvr *localRcvr) handleMsg(mh msgAndHandler) {
	mh.handler.Recv(mh.msg, &rcvr.ctx)
}

func (rcvr *localRcvr) handleCmd(cmd actorCommand) {
	switch cmd {
	case stopActor:
		close(rcvr.dataCh)
		close(rcvr.ctrlCh)
		close(rcvr.waitCh)
	}
}

func (rcvr *localRcvr) enque(mh msgAndHandler) {
	rcvr.dataCh <- mh
}

func (mapr *mapper) start() {
	for {
		select {
		case d, ok := <-mapr.dataCh:
			if !ok {
				return
			}
			mapr.handleMsg(d)

		case cmd, ok := <-mapr.ctrlCh:
			if !ok {
				return
			}
			mapr.handleCmd(cmd)
		}
	}
}

func (mapr *mapper) closeChannels() {
	close(mapr.dataCh)
	close(mapr.ctrlCh)
	close(mapr.waitCh)
}

func (mapr *mapper) stopReceivers() {
	// TODO(soheil): Impl this method.
}

func (mapr *mapper) handleCmd(cmd actorCommand) {
	switch cmd {
	case stopActor:
		mapr.stopReceivers()
		mapr.closeChannels()
	}
}

func (mapr *mapper) receiver(dk DictionaryKey) receiver {
	return mapr.receivers[dk.String()]
}

func (mapr *mapper) setReceiver(dk DictionaryKey, rcvr receiver) {
	mapr.receivers[dk.String()] = rcvr
}

func (mapr *mapper) syncReceivers(ms MapSet, rcvr receiver) {
	for _, dictKey := range ms {
		dkRecvr := mapr.receiver(dictKey)
		if dkRecvr == nil {
			mapr.lockKey(dictKey, rcvr)
			continue
		}

		if dkRecvr == rcvr {
			continue
		}

		glog.Fatalf("Incosistent shards for keys %v in MapSet %v", dictKey,
			ms)
	}
}

func (mapr *mapper) anyReceiver(ms MapSet) receiver {
	for _, dictKey := range ms {
		rcvr := mapr.receiver(dictKey)
		if rcvr != nil {
			return rcvr
		}
	}

	return nil
}

func (mapr *mapper) handleMsg(mh msgAndHandler) {
	mapSet := mh.handler.Map(mh.msg, &mapr.ctx)

	rcvr := mapr.anyReceiver(mapSet)

	if rcvr == nil {
		rcvr = mapr.newReceiver(mapSet)
	}

	mapr.syncReceivers(mapSet, rcvr)

	rcvr.enque(mh)
}

// Locks the map set and returns a new receiver ID if possible, otherwise
// returns the ID of the owner of this map set.
func (mapr *mapper) tryLock(mapSet MapSet) ReceiverId {
	mapr.lastRId++
	id := ReceiverId{
		StageId:   mapr.ctx.stage.Id(),
		RcvrId:    mapr.lastRId,
		ActorName: mapr.ctx.actor.Name(),
	}

	if mapr.ctx.stage.isIsol() {
		return id
	}

	v := mapr.ctx.stage.registery.storeOrGet(id, mapSet)

	if v.StageId == id.StageId && v.RcvrId == id.RcvrId {
		return id
	}

	mapr.lastRId--
	id.StageId = v.StageId
	id.RcvrId = v.RcvrId
	return id
}

func (mapr *mapper) lockKey(dk DictionaryKey, rcvr receiver) bool {
	mapr.setReceiver(dk, rcvr)
	if mapr.ctx.stage.isIsol() {
		return true
	}

	mapr.ctx.stage.registery.storeOrGet(rcvr.id(), []DictionaryKey{dk})

	return true
}

func (mapr *mapper) isLocalRcvr(id ReceiverId) bool {
	return mapr.ctx.stage.Id() == id.StageId
}

func (mapr *mapper) newLocalRcvr(id ReceiverId) localRcvr {
	r := localRcvr{
		asyncRoutine: asyncRoutine{
			dataCh: make(chan msgAndHandler, cap(mapr.dataCh)),
			ctrlCh: make(chan actorCommand),
			waitCh: make(chan interface{}),
		},
		rId: id,
	}
	r.ctx = recvContext{
		context: mapr.ctx,
	}
	return r
}

func (mapr *mapper) newReceiver(mapSet MapSet) receiver {
	var rcvr receiver
	rcvrId := mapr.tryLock(mapSet)
	if mapr.isLocalRcvr(rcvrId) {
		fmt.Println("Creating a local receiver")
		r := mapr.newLocalRcvr(rcvrId)
		r.ctx.rcvr = &r
		rcvr = &r
	} else {
		fmt.Println("Creating a proxy receiver")
		r := proxyRcvr{
			localRcvr: mapr.newLocalRcvr(rcvrId),
		}
		r.ctx.rcvr = &r
		rcvr = &r
	}
	go rcvr.start()

	for _, dictKey := range mapSet {
		mapr.setReceiver(dictKey, rcvr)
	}

	return rcvr
}
