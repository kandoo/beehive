/*
This is a distributed actor system.

TODO(soheil): Add a complete description.

On each stage, we run the same collection of actor, but each control specific
shards throughout the cluster.

[Shard A1]  [Shard A2]   [Shard B1]
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
   [Shard A3]    [Shard B3]  [Shard B4]
*/

package actor

import (
  "github.com/golang/glog"
)

type StageId uint32

type StageStatus int

// Valid values for StageStatus.
const (
  STOPPED StageStatus = iota
  STARTED             = iota
)

type Stage interface {
  Id() StageId

  // Starts the stage using the options provided in program arguments.
  Start()
  // Stops the stage and all its actors.
  Stop()
  // Returns the stage status.
  Status() StageStatus

  // Creates an actor with the given name. Note that actors are not active until
  // the stage is started.
  NewActor(name ActorName) Actor
}

type ActorId uint32
type ActorName string

// Message
type Msg interface {
  Reply(msg Msg) error
  Type() MsgType
}

type MsgType string

// State is the storage for a collection of dictionaries.
type State interface {
  Dict(name DictionaryName) Dictionary
}

// Simply a key-value store.
type Dictionary interface {
  Name() []byte
  Get(key Key) (Value, error)
  Set(key Key, val Value)
}

// DictionaryName is the key to lookup dictionaries in the state.
type DictionaryName []byte

// Key is to lookup values in Dicitonaries and is simply a byte array.
type Key []byte
type Value interface{}

// A tuple of (dictionary name, key) which is used in the map function of
// actors.
type DictionaryKey struct {
  Dict DictionaryName
  Key  Key
}

type MapSet []DictionaryKey

// An applications map function that maps a specific message to the set of keys
// in state dictionaries. This method is assumed not to be thread-safe and is
// called sequentially.
type Map func(m Msg, s State) MapSet

// An application recv function that handles a message. This method is called in
// parallel for different map-sets and sequentially within a map-set.
type Recv func(m Msg, s State)

type Handler struct {
  Map   Map
  Recv  Recv
  actor Actor
}

// Actors simply process and exchange messages. Actor methods are not
// thread-safe and we assume that neither are its map and receive functions.
type Actor interface {
  // Registers a map function for the given message type. A call to this
  // method is usually accompanied with a call to Recv for the same message
  // type. If the receive function was not provided, the message type will be
  // ignored.
  Map(t MsgType, m Map)
  // Registers a receive function for the given message type. A call to this
  // method is usually accompanied with a call to Map for the same message type.
  // If the map function was not provided, we set map to a function that makes
  // the recv function centralized.
  Recv(t MsgType, r Recv)
  // Hanldes a specific message type using the map and receive functions.
  HandleFunc(t MsgType, m Map, r Recv)
  // Handles a specific message type using the handler.
  Handle(t MsgType, h Handler)

  // Returns the state used in the map function.
  mapState() State
  // Returns the state of this actor that is shared among all instances.
  state() State
  // Creates a state for a specific shard.
  newShardState() State
}

type actor struct {
  instances []byte // of actorInstance
}

type actorInstance interface {
}

type localActorInstance struct {
}

type proxyActorInstance struct {
}

func processMessage(m Msg) {
  for _, handler := range lookupHandlers(m.Type()) {
    callHandler(handler, m)
  }
}

type Command int

const (
  Stop  Command = iota
  Start         = iota
)

type Shard struct {
  recvCh chan Msg
  ctrlCh chan Command
  state  State
}

func (h *Handler) callHandler(m Msg) {
  mapSet := h.Map(m, h.actor.mapState())

  var sh *Shard = nil
  for _, dictKey := range mapSet {
    tempSh, ok := h.actor.findShard(dictKey)
    if (!ok && sh == nil) || (ok && tempSh == sh) {
      continue
    }

    if !ok {
      h.actor.setShard(dictKey, sh)
      continue
    }

    glog.Fatalf("Incosistent shards for keys %v in MapSet %v", dictKey,
      mapSet)
  }

  if sh == nil {
    sh := &Shard{
      recvCh: make(chan Msg),
      ctrlCh: make(chan Command),
      state:  h.actor.newShardState(),
    }

    for _, dictKey := range mapSet {
      if _, ok := h.actor.findShard(dictKey); ok {
        glog.Fatalf(
          "Incosistent channels for keys %v in MapSet %v", dictKey, mapSet)
      }

      h.actor.setShard(dictKey, sh)
    }

    go (func() {
      var recvFunc Recv
      if owner, locked := a.tryLock(m, mapSet); locked {
        recvFunc = h.Recv
      } else {
        recvFunc = proxyRecvFunc(owner)
      }

      for {
        select {
        case msg := <-sh.recvCh:
          recvFunc(msg, sh.state)

        case cmd := <-sh.ctrlCh:
          switch cmd {
          case Stop:
            for cmd = range sh.ctrlCh {
              if cmd == Start {
                break
              }
            }
          }
        }
      }
    })()
  }

  sh.recvCh <- m
}

func init() {
  var s Stage
  a := s.NewActor("Name")

  SoheilEvent := MsgType("test")

  a.Recv(SoheilEvent, func(e Msg, s State) {
  })

  a.Map(SoheilEvent, func(e Msg, s State) MapSet {
    return MapSet{{}}
  })

  a.Handle(SoheilEvent, Handler{Map: nil, Recv: nil})

  s.Start()
}
