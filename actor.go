/*
This is a distributed actor system.

TODO(soheil): Add a complete description.

On each stage, we run the same collection of actor, but each control specific
shards throughout the cluster.

[Inst. A1] [Inst. A2]    [Inst. B1]
    |      /                 |
   [Actor A]             [Actor B]
 ----------------------------------
|             Stage 1              |
 ----------------------------------
                |
 ----------------------------------
|             Stage 2              |
 ----------------------------------
   [Actor A]             [Actor B]
       |                 /      |
   [Inst. A3]    [Inst. B3]  [Inst. B4]
*/

package actor

import (
  "errors"
  "reflect"

  "github.com/golang/glog"
)

// Represents the globally unique ID of a stage. These IDs are automatically
// assigned using the distributed configuration service.
type StageId uint32

// Status of a stage.
type StageStatus int

// Valid values for StageStatus.
const (
  StageStopped StageStatus = iota
  StageStarted             = iota
)

type Stage interface {
  // ID of the stage. Valid only if the stage is started.
  Id() StageId

  // Starts the stage and will close the waitCh once the stage stops.
  Start(waitCh chan interface{}) error
  // Stops the stage and all its actors.
  Stop() error
  // Returns the stage status.
  Status() StageStatus

  // Creates an actor with the given name. Note that actors are not active until
  // the stage is started.
  NewActor(name ActorName) Actor

  // Emits a message from this stage.
  Emit(msg Msg)
  // Sends a message to a specific actor that owns a specific MapSet. Returns an
  // error if there is no such actor.
  SendTo(msg Msg, to ActorName, ms MapSet)
  // Replies to a message.
  ReplyTo(msg Msg, reply Msg)
}

// Creates a new stage based on the given configuration.
func NewStage(cfg StageConfig) Stage {
  return &stage{
    id:      0,
    status:  StageStopped,
    config:  cfg,
    dataCh:  make(chan Msg, cfg.DataChBufSize),
    ctrlCh:  make(chan StageCmd),
    actors:  make([]Actor, 0),
    mappers: make(map[MsgType][]mapperAndHandler),
  }
}

// Configuration of a stage.
type StageConfig struct {
  // Listening address of the stage.
  StageAddr string
  // Reigstery service address.
  RegAddr string
  // Buffer size in the data channel.
  DataChBufSize int
}

type StageCmd int

const (
  StopStage  StageCmd = iota
  DrainStage          = iota
)

type ActorName string

type mapperAndHandler struct {
  mapr    *mapper
  handler Handler
}

type stage struct {
  id      StageId
  status  StageStatus
  config  StageConfig
  dataCh  chan Msg
  ctrlCh  chan StageCmd
  actors  []Actor
  mappers map[MsgType][]mapperAndHandler
}

func (s *stage) Id() StageId {
  return s.id
}

func (s *stage) closeChannels() {
  for _, mhs := range s.mappers {
    for _, mh := range mhs {
      mh.mapr.ctrlCh <- stopActor
      <-mh.mapr.waitCh
    }
  }
  close(s.dataCh)
  close(s.ctrlCh)
}

func (s *stage) handleCmd(cmd StageCmd) {
  switch cmd {
  case StopStage:
    close(s.dataCh)
    close(s.ctrlCh)
    return
  case DrainStage:
    // TODO(soheil): Implement drain.
  }
}

func (s *stage) ReplyTo(msg Msg, reply Msg) {
  // TODO(soheil): implement this.
  // Lookup actor from msg.
  // Ask the mapper to send the message to that specific recvr.
  glog.Fatalf("stage.ReplyTo is not implemented.")
}

const (
  kInitialMappers   = 10
  kInitialReceivers = 10
)

func (s *stage) registerActor(a Actor) {
  s.actors = append(s.actors, a)
}

func (s *stage) registerHandler(t MsgType, m *mapper, h Handler) {
  s.mappers[t] = append(s.mappers[t], mapperAndHandler{m, h})
}

func (s *stage) handleMsg(msg Msg) {
  glog.Infof("Mapper %v %v", s.mappers, msg.Type())
  for _, mh := range s.mappers[msg.Type()] {
    glog.Infof("Sent data %v %v", msg, mh.mapr)
    mh.mapr.dataCh <- msgAndHandler{msg, mh.handler}
  }
}

func (s *stage) Start(waitCh chan interface{}) error {
  // TODO(soheil): Listen and grab and ID. Connect to etcd.
  defer close(waitCh)
  s.status = StageStarted
  for {
    select {
    case msg, ok := <-s.dataCh:
      if !ok {
        return errors.New("Data channel is closed.")
      }
      glog.V(2).Infof("Received msg %v", msg)
      s.handleMsg(msg)
    case cmd, ok := <-s.ctrlCh:
      if !ok {
        return errors.New("Control channel is closed.")
      }
      s.handleCmd(cmd)
    case <-waitCh:
      glog.Fatalf("Stage's wait channel should not be closed.")
    }
  }
  return nil
}

func (s *stage) Stop() error {
  if s.ctrlCh == nil {
    return errors.New("Control channel is closed.")
  }

  if s.Status() == StageStopped {
    return errors.New("Stage is already stopped.")
  }

  s.ctrlCh <- StopStage
  return nil
}

func (s *stage) Status() StageStatus {
  return s.status
}

func (s *stage) NewActor(name ActorName) Actor {
  a := &actor{
    name:  name,
    stage: s,
  }
  a.initMapper()
  return a
}

func (s *stage) Emit(msg Msg) {
  s.dataCh <- msg
}

func (s *stage) SendTo(msg Msg, to ActorName, ms MapSet) {
}

type ReceiverId struct {
  StageId   StageId
  ActorName ActorName
  RcvrId    int32
}

// Creates a global ID from stage and receiver IDs.
//func MakeGlobalReceiverId(sId StageId, rId ReceiverId) GlobalReceiverId {
//return GlobalReceiverId(sId)<<32 | GlobalReceiverId(rId)
//}

// Extracts actor ID from a global receiver ID.
//func ExtractReceiverId(gId GlobalReceiverId) ReceiverId {
//return ReceiverId(gId & 0xFFFFFFFF)
//}

// Extracts stage ID from a global receiver ID.
//func ExtractStageId(gId GlobalReceiverId) StageId {
//return StageId((gId >> 32) & 0xFFFFFFFF)
//}

// Message is a generic interface for messages emitted in the system. Messages
// are defined for each type.
type Msg interface {
  Reply(msg Msg)
  Type() MsgType
}

type MsgType string

type GenericMsg struct {
  stage  Stage
  rcvrId ReceiverId
}

func (m *GenericMsg) Reply(r Msg) {
  if m.stage == nil {
    glog.Fatalf(
      "Message not correctly build: No stage in the generic message.")
  }

  m.stage.ReplyTo(m, r)
}

const (
  genericMsgType = "GenericMsg"
)

// This method should always be overridden by messages.
func (m *GenericMsg) Type() MsgType {
  return genericMsgType
}

// State is the storage for a collection of dictionaries.
type State interface {
  Dict(name DictionaryName) Dictionary
}

// Simply a key-value store.
type Dictionary interface {
  Name() DictionaryName
  Get(key Key) (Value, bool)
  Set(key Key, val Value)
}

// DictionaryName is the key to lookup dictionaries in the state.
type DictionaryName string

// Key is to lookup values in Dicitonaries and is simply a byte array.
type Key string
type Value interface{}

// A tuple of (dictionary name, key) which is used in the map function of
// actors.
type DictionaryKey struct {
  Dict DictionaryName
  Key  Key
}

type MapSet []DictionaryKey

func dictionaryKeyToString(dk DictionaryKey) string {
  // TODO(soheil): This will change when we implement it using a Trie instead of
  // a map.
  return string(dk.Dict) + "/" + string(dk.Key)
}

type Context interface {
  State() State
}

type RecvContext interface {
  Context
  NewGenericMsg() GenericMsg
  Emit(msg Msg)
  SendTo(msg Msg, to ActorName, ms MapSet)
  ReplyTo(msg Msg, r Msg)
}

type context struct {
  state State
  stage Stage
  actor Actor
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

func (ctx *recvContext) NewGenericMsg() GenericMsg {
  return GenericMsg{stage: ctx.stage, rcvrId: ctx.rcvr.id()}
}

func (ctx *recvContext) Emit(m Msg) {
  gmsg := m.(*GenericMsg)
  gmsg.stage = ctx.stage
  gmsg.rcvrId = ctx.rcvr.id()
  ctx.stage.Emit(m)
}

func (ctx *recvContext) SendTo(msg Msg, to ActorName, ms MapSet) {
  // TODO(soheil): Implement send to.
  glog.Fatal("Sendto is not implemented.")
}

func (ctx *recvContext) ReplyTo(msg Msg, r Msg) {
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

func (a *actor) Handle(msgType interface{}, h Handler) {
  t, ok := msgType.(MsgType)
  if !ok {
    t = MsgType(reflect.TypeOf(msgType).String())
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
  glog.Infof("Started %v", rcvr.dataCh)
  for {
    select {
    case d, ok := <-rcvr.dataCh:
      if !ok {
        return
      }
      glog.Infof("Message got in rcvr %v", d)
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
  glog.Infof("Enqueued ")
  rcvr.dataCh <- mh
}

type proxyRcvr struct {
  localRcvr
}

func (rcvr *proxyRcvr) handleMsg(mh msgAndHandler) {
}

func (rcvr *proxyRcvr) start() {
  glog.Infof("It's a proxy!!!!")
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

func (mapr *mapper) findReceiver(dk DictionaryKey) (receiver, bool) {
  return nil, false
}

func (mapr *mapper) setReceiver(dk DictionaryKey, rcvr receiver) {
}

func (mapr *mapper) handleMsg(mh msgAndHandler) {
  mapSet := mh.handler.Map(mh.msg, &mapr.ctx)

  var rcvr receiver
  for _, dictKey := range mapSet {
    tempRr, ok := mapr.findReceiver(dictKey)
    if ok && tempRr == rcvr {
      continue
    }

    if !ok {
      mapr.lockKey(dictKey, rcvr)
      continue
    }

    glog.Fatalf("Incosistent shards for keys %v in MapSet %v", dictKey,
      mapSet)
  }

  if rcvr == nil {
    rcvr = mapr.newReceiver(mapSet)
    glog.Info("New recvr %v", rcvr)
  }

  rcvr.enque(mh)
}

// Locks the map set and returns a new receiver ID if possible, otherwise
// returns the ID of the owner of this map set.
func (mapr *mapper) tryLock(mapSet MapSet) ReceiverId {
  return ReceiverId{}
}

func (mapr *mapper) lockKey(dk DictionaryKey, rcvr receiver) bool {
  return false
}

func proxyRecv(sId StageId, rId ReceiverId) Recv {
  return nil
}

func (mapr *mapper) isLocalRcvr(id ReceiverId) bool {
  return mapr.ctx.stage.Id() == id.StageId
}

func (mapr *mapper) newReceiver(mapSet MapSet) receiver {
  var rcvr receiver
  rcvrId := mapr.tryLock(mapSet)
  if mapr.isLocalRcvr(rcvrId) {
    // TODO(soheil): State should include rcvr's id.
    //state := newState(string(mapr.ctx.actor.Name()))
    r := &localRcvr{
      asyncRoutine: asyncRoutine{
        dataCh: make(chan msgAndHandler, cap(mapr.dataCh)),
        ctrlCh: make(chan actorCommand),
        waitCh: make(chan interface{}),
      },
      rId: rcvrId,
    }
    r.ctx = recvContext{
      context: context{
        stage: mapr.ctx.stage,
        actor: mapr.ctx.actor,
      },
      rcvr: r,
    }
    go r.start()
    glog.Infof("Local started")
    rcvr = r
  } else {
    //h := proxyRecv(rcvr, mapr.ctx)
    //rcvr = newReceiver(proxyRecv(rcvr.id), nil, rcvr.id)
    // TODO(soheil): Implement proxy messages.
    rcvr = &proxyRcvr{}
    glog.Fatalf("Proxy messages are not implemented yet!")
  }

  for _, dictKey := range mapSet {
    mapr.setReceiver(dictKey, rcvr)
  }

  return rcvr
}

func init() {
}
