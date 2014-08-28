package bh

import (
	"encoding/gob"
	"errors"
	"flag"
	"net"
	"os"
	"time"

	"github.com/golang/glog"
)

// StageId represents the globally unique ID of a stage. These IDs are
// automatically assigned using the distributed configuration service.
type StageId string

// StageStatus represents the status of a stage.
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
	Start(joinCh chan interface{}) error
	// Stops the stage and all its actors.
	Stop() error
	// Returns the stage status.
	Status() StageStatus

	// Creates an actor with the given name. Note that actors are not active until
	// the stage is started.
	NewActor(name ActorName) Actor

	// Emits a message containing msgData from this stage.
	Emit(msgData interface{})
	// Sends a message to a specific receiver that owns a specific dictionary key.
	SendToDictKey(msgData interface{}, to ActorName, dk DictionaryKey)
	// Sends a message to a sepcific receiver.
	SentToRcvr(msgData interface{}, to RcvrId)
	// Replies to a message.
	ReplyTo(msg Msg, replyData interface{}) error

	// Registers a message for encoding/decoding. This method should be called
	// only on messages that have no active handler. Such messages are almost
	// always replies to some detached handler.
	RegisterMsg(msg interface{})
}

// Configuration of a stage.
type StageConfig struct {
	// Listening address of the stage.
	StageAddr string
	// Reigstery service addresses.
	RegAddrs []string
	// Buffer size in the data channel.
	DataChBufSize int
	// Whether to instrument actors on the stage.
	Instrument bool
}

// Creates a new stage based on the given configuration.
func NewStageWithConfig(cfg StageConfig) Stage {
	s := &stage{
		id:      StageId(cfg.StageAddr),
		status:  StageStopped,
		config:  cfg,
		dataCh:  make(chan *msg, cfg.DataChBufSize),
		ctrlCh:  make(chan StageCmd),
		actors:  make(map[ActorName]*actor, 0),
		mappers: make(map[MsgType][]mapperAndHandler),
	}

	s.init()
	go s.listen()

	return s
}

func (s *stage) init() {
	gob.Register(inMemoryDictionary{})
	gob.Register(inMemoryState{})
	gob.Register(msg{})

	if s.config.Instrument {
		s.collector = newActorStatCollector(s)
	} else {
		s.collector = &dummyStatCollector{}
	}
}

var (
	DefaultCfg = StageConfig{}
)

// Create a new stage and load its configuration from command line flags.
func NewStage() Stage {
	if !flag.Parsed() {
		flag.Parse()
	}

	return NewStageWithConfig(DefaultCfg)
}

type StageCmd int

const (
	StopStage  StageCmd = iota
	DrainStage          = iota
)

func init() {
	flag.StringVar(&DefaultCfg.StageAddr, "laddr", "localhost:7767",
		"The listening address used to communicate with other nodes.")
	flag.Var(&commaSeparatedValue{&DefaultCfg.RegAddrs}, "raddrs",
		"Address of etcd machines. Separate entries with a semi-colon ';'")
	flag.IntVar(&DefaultCfg.DataChBufSize, "chsize", 1024,
		"Buffer size of channels.")
	flag.BoolVar(&DefaultCfg.Instrument, "instrument", false,
		"Whether to insturment actors.")
}

const (
	kInitialMappers   = 10
	kInitialReceivers = 10
)

type mapperAndHandler struct {
	mapr    *mapper
	handler Handler
}

// The internal implementation of Stage.
type stage struct {
	id     StageId
	status StageStatus
	config StageConfig

	dataCh chan *msg
	ctrlCh chan StageCmd
	sigCh  chan os.Signal

	actors  map[ActorName]*actor
	mappers map[MsgType][]mapperAndHandler

	registery registery
	collector statCollector

	listener net.Listener
}

func (s *stage) Id() StageId {
	return s.id
}

func (s *stage) RegisterMsg(msg interface{}) {
	gob.Register(msg)
}

func (s *stage) isIsol() bool {
	return s.registery.connected()
}

func (s *stage) actor(name ActorName) (*actor, bool) {
	a, ok := s.actors[name]
	return a, ok
}

func (s *stage) closeChannels() {
	glog.Info("Closing the stage listener...")
	s.listener.Close()

	glog.Info("Stopping mappers...")
	stopCh := make(chan asyncResult)
	maprs := make(map[*mapper]bool)
	for _, mhs := range s.mappers {
		for _, mh := range mhs {
			maprs[mh.mapr] = true
		}
	}

	for m, _ := range maprs {
		m.ctrlCh <- routineCmd{stopCmd, nil, stopCh}
		glog.V(3).Infof("Waiting on a mapper: %p", m)
		select {
		case res := <-stopCh:
			_, err := res.get()
			if err != nil {
				glog.Errorf("Error in stopping a mapper: %v", err)
			}
		case <-time.After(time.Second * 1):
			glog.Info("Still waiting for a mapper...")
		}
	}
	close(s.dataCh)
	close(s.ctrlCh)
	close(s.sigCh)
}

func (s *stage) handleCmd(cmd StageCmd) {
	switch cmd {
	case StopStage:
		// TODO(soheil): This has a race with Stop(). Use atomics here.
		s.status = StageStopped
		s.closeChannels()
		//close(s.dataCh)
		//close(s.ctrlCh)
		//close(s.sigCh)
		return
	case DrainStage:
		// TODO(soheil): Implement drain.
		glog.Fatalf("Drain Stage is not implemented.")
	}
}

func (s *stage) registerActor(a *actor) {
	s.actors[a.Name()] = a
}

func (s *stage) registerHandler(t MsgType, m *mapper, h Handler) {
	s.mappers[t] = append(s.mappers[t], mapperAndHandler{m, h})
}

func (s *stage) handleMsg(m *msg) {
	for _, mh := range s.mappers[m.Type()] {
		mh.mapr.dataCh <- msgAndHandler{m, mh.handler}
	}
}

func (s *stage) startMappers() {
	for _, a := range s.actors {
		go a.mapper.start()
	}
}

func (s *stage) Start(joinCh chan interface{}) error {
	defer close(joinCh)

	s.status = StageStarted
	s.registerSignals()
	s.connectToRegistery()
	s.startMappers()

	for {
		select {
		case msg, ok := <-s.dataCh:
			if !ok {
				return errors.New("Data channel is closed.")
			}
			s.handleMsg(msg)
		case cmd, ok := <-s.ctrlCh:
			if !ok {
				return errors.New("Control channel is closed.")
			}
			s.handleCmd(cmd)
		case <-joinCh:
			glog.Fatalf("Stage's join channel should not be used nor closed.")
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
		name:     name,
		stage:    s,
		handlers: make(map[MsgType]Handler),
	}
	a.initMapper()
	s.registerActor(a)
	return a
}

func (s *stage) Emit(msgData interface{}) {
	s.emitMsg(&msg{MsgData: msgData, MsgType: msgType(msgData)})
}

func (s *stage) emitMsg(msg *msg) {
	switch {
	case msg.isBroadCast():
		s.dataCh <- msg
	case msg.isUnicast():
		a, ok := s.actor(msg.To().ActorName)
		if !ok {
			glog.Fatalf("Application not found: %s", msg.To().ActorName)
		}
		a.mapper.dataCh <- msgAndHandler{msg, a.handler(msg.Type())}
	}
}

func (s *stage) SendToDictKey(msgData interface{}, to ActorName,
	dk DictionaryKey) {
	// TODO(soheil): Implement this stage.SendTo.
	glog.Fatalf("Not implemented yet.")
}

func (s *stage) SentToRcvr(msgData interface{}, to RcvrId) {
	s.emitMsg(newMsgFromData(msgData, RcvrId{}, to))
}

// Reply to thatMsg with the provided replyData.
func (s *stage) ReplyTo(thatMsg Msg, replyData interface{}) error {
	m := thatMsg.(*msg)
	if m.NoReply() {
		return errors.New("Cannot reply to this message.")
	}

	s.emitMsg(newMsgFromData(replyData, RcvrId{}, m.From()))
	return nil
}
