package actor

import (
	"errors"
	"flag"
	"os"

	"github.com/golang/glog"
)

// Represents the globally unique ID of a stage. These IDs are automatically
// assigned using the distributed configuration service.
type StageId string

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

	// Emits a message containing msgData from this stage.
	Emit(msgData interface{})
	// Sends a message to a specific actor that owns a specific dictionary key.
	SendTo(msgData interface{}, to ActorName, dk DictionaryKey)
	// Replies to a message.
	ReplyTo(msg Msg, replyData interface{})

	// Emits a message from this stage.
	EmitMsg(msg Msg)
	// Sends a message to the actor instance that owns the dictionary key.
	SendMsgTo(msg Msg, to ActorName, dk DictionaryKey)
	// Replies to a message with another message.
	ReplyMsgTo(msg Msg, reply Msg)
}

// Configuration of a stage.
type StageConfig struct {
	// Listening address of the stage.
	StageAddr string
	// Reigstery service addresses.
	RegAddrs []string
	// Buffer size in the data channel.
	DataChBufSize int
}

// Creates a new stage based on the given configuration.
func NewStageWithConfig(cfg StageConfig) Stage {
	s := &stage{
		id:      StageId(cfg.StageAddr),
		status:  StageStopped,
		config:  cfg,
		dataCh:  make(chan Msg, cfg.DataChBufSize),
		ctrlCh:  make(chan StageCmd),
		actors:  make([]Actor, 0),
		mappers: make(map[MsgType][]mapperAndHandler),
	}

	go s.listen()

	return s
}

var (
	defaultCfg = StageConfig{}
)

// Create a new stage and load its configuration from command line flags.
func NewStage() Stage {
	if !flag.Parsed() {
		flag.Parse()
	}

	return NewStageWithConfig(defaultCfg)
}

type StageCmd int

const (
	StopStage  StageCmd = iota
	DrainStage          = iota
)

func init() {
	flag.StringVar(&defaultCfg.StageAddr, "laddr", "localhost:7767",
		"The listening address used to communicate with other nodes.")
	flag.Var(&commaSeparatedValue{&defaultCfg.RegAddrs}, "raddrs",
		"Address of etcd machines. Separate entries with a semi-colon ';'")
	flag.IntVar(&defaultCfg.DataChBufSize, "chsize", 1024,
		"Buffer size of channels.")
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

	dataCh chan Msg
	ctrlCh chan StageCmd
	sigCh  chan os.Signal

	actors  []Actor
	mappers map[MsgType][]mapperAndHandler

	registery registery
}

func (s *stage) Id() StageId {
	return s.id
}

func (s *stage) isIsol() bool {
	return s.registery.connected()
}

func (s *stage) actor(name ActorName) (*actor, error) {
	for _, a := range s.actors {
		if a.Name() == name {
			return a.(*actor), nil
		}
	}

	return nil, errors.New("Actor not found.")
}

func (s *stage) closeChannels() {
	for _, mhs := range s.mappers {
		for _, mh := range mhs {
			mh.mapr.ctrlCh <- routineCmd{cmdType: stopRoutine}
			<-mh.mapr.waitCh
		}
	}
	close(s.dataCh)
	close(s.ctrlCh)
}

func (s *stage) handleCmd(cmd StageCmd) {
	switch cmd {
	case StopStage:
		// TODO(soheil): This has a race with Stop(). Use atomics here.
		s.status = StageStopped
		close(s.dataCh)
		close(s.ctrlCh)
		close(s.sigCh)
		return
	case DrainStage:
		// TODO(soheil): Implement drain.
	}
}

func (s *stage) registerActor(a Actor) {
	s.actors = append(s.actors, a)
}

func (s *stage) registerHandler(t MsgType, m *mapper, h Handler) {
	s.mappers[t] = append(s.mappers[t], mapperAndHandler{m, h})
}

func (s *stage) handleMsg(msg Msg) {
	for _, mh := range s.mappers[msg.Type()] {
		mh.mapr.dataCh <- msgAndHandler{msg, mh.handler}
	}
}

func (s *stage) Start(waitCh chan interface{}) error {
	// TODO(soheil): Listen and grab and ID. Connect to etcd.
	defer close(waitCh)

	s.status = StageStarted
	s.registerSignals()
	s.connectToRegistery()

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
		case <-waitCh:
			glog.Fatalf("Stage's wait channel should not be used nor closed.")
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

func (s *stage) Emit(msgData interface{}) {
	s.EmitMsg(&msg{MsgData: msgData, MsgType: msgType(msgData)})
}

func (s *stage) EmitMsg(msg Msg) {
	s.dataCh <- msg
}

func (s *stage) SendTo(msgData interface{}, to ActorName, dk DictionaryKey) {
}

func (s *stage) ReplyTo(msg Msg, replyData interface{}) {
}

func (s *stage) SendMsgTo(msg Msg, to ActorName, dk DictionaryKey) {
}

func (s *stage) ReplyMsgTo(msg Msg, reply Msg) {
}
