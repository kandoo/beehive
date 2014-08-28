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

// HiveId represents the globally unique ID of a hive. These IDs are
// automatically assigned using the distributed configuration service.
type HiveId string

// HiveStatus represents the status of a hive.
type HiveStatus int

// Valid values for HiveStatus.
const (
	HiveStopped HiveStatus = iota
	HiveStarted            = iota
)

type Hive interface {
	// ID of the hive. Valid only if the hive is started.
	Id() HiveId

	// Starts the hive and will close the waitCh once the hive stops.
	Start(joinCh chan interface{}) error
	// Stops the hive and all its apps.
	Stop() error
	// Returns the hive status.
	Status() HiveStatus

	// Creates an app with the given name. Note that apps are not active until
	// the hive is started.
	NewApp(name AppName) App

	// Emits a message containing msgData from this hive.
	Emit(msgData interface{})
	// Sends a message to a specific receiver that owns a specific dictionary key.
	SendToDictKey(msgData interface{}, to AppName, dk DictionaryKey)
	// Sends a message to a sepcific receiver.
	SentToRcvr(msgData interface{}, to RcvrId)
	// Replies to a message.
	ReplyTo(msg Msg, replyData interface{}) error

	// Registers a message for encoding/decoding. This method should be called
	// only on messages that have no active handler. Such messages are almost
	// always replies to some detached handler.
	RegisterMsg(msg interface{})
}

// Configuration of a hive.
type HiveConfig struct {
	// Listening address of the hive.
	HiveAddr string
	// Reigstery service addresses.
	RegAddrs []string
	// Buffer size in the data channel.
	DataChBufSize int
	// Whether to instrument apps on the hive.
	Instrument bool
}

// Creates a new hive based on the given configuration.
func NewHiveWithConfig(cfg HiveConfig) Hive {
	h := &hive{
		id:      HiveId(cfg.HiveAddr),
		status:  HiveStopped,
		config:  cfg,
		dataCh:  make(chan *msg, cfg.DataChBufSize),
		ctrlCh:  make(chan HiveCmd),
		apps:    make(map[AppName]*app, 0),
		mappers: make(map[MsgType][]mapperAndHandler),
	}

	h.init()
	go h.listen()

	return h
}

func (h *hive) init() {
	gob.Register(inMemoryDictionary{})
	gob.Register(inMemoryState{})
	gob.Register(msg{})

	if h.config.Instrument {
		h.collector = newAppStatCollector(h)
	} else {
		h.collector = &dummyStatCollector{}
	}
}

var (
	DefaultCfg = HiveConfig{}
)

// Create a new hive and load its configuration from command line flags.
func NewHive() Hive {
	if !flag.Parsed() {
		flag.Parse()
	}

	return NewHiveWithConfig(DefaultCfg)
}

type HiveCmd int

const (
	StopHive  HiveCmd = iota
	DrainHive         = iota
)

func init() {
	flag.StringVar(&DefaultCfg.HiveAddr, "laddr", "localhost:7767",
		"The listening address used to communicate with other nodes.")
	flag.Var(&commaSeparatedValue{&DefaultCfg.RegAddrs}, "raddrs",
		"Address of etcd machines. Separate entries with a semi-colon ';'")
	flag.IntVar(&DefaultCfg.DataChBufSize, "chsize", 1024,
		"Buffer size of channels.")
	flag.BoolVar(&DefaultCfg.Instrument, "instrument", false,
		"Whether to insturment apps.")
}

const (
	kInitialMappers   = 10
	kInitialReceivers = 10
)

type mapperAndHandler struct {
	mapr    *mapper
	handler Handler
}

// The internal implementation of Hive.
type hive struct {
	id     HiveId
	status HiveStatus
	config HiveConfig

	dataCh chan *msg
	ctrlCh chan HiveCmd
	sigCh  chan os.Signal

	apps    map[AppName]*app
	mappers map[MsgType][]mapperAndHandler

	registery registery
	collector statCollector

	listener net.Listener
}

func (h *hive) Id() HiveId {
	return h.id
}

func (h *hive) RegisterMsg(msg interface{}) {
	gob.Register(msg)
}

func (h *hive) isIsol() bool {
	return h.registery.connected()
}

func (h *hive) app(name AppName) (*app, bool) {
	a, ok := h.apps[name]
	return a, ok
}

func (h *hive) closeChannels() {
	glog.Info("Closing the hive listener...")
	h.listener.Close()

	glog.Info("Stopping mappers...")
	stopCh := make(chan asyncResult)
	maprs := make(map[*mapper]bool)
	for _, mhs := range h.mappers {
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
	close(h.dataCh)
	close(h.ctrlCh)
	close(h.sigCh)
}

func (h *hive) handleCmd(cmd HiveCmd) {
	switch cmd {
	case StopHive:
		// TODO(soheil): This has a race with Stop(). Use atomics here.
		h.status = HiveStopped
		h.closeChannels()
		//close(h.dataCh)
		//close(h.ctrlCh)
		//close(h.sigCh)
		return
	case DrainHive:
		// TODO(soheil): Implement drain.
		glog.Fatalf("Drain Hive is not implemented.")
	}
}

func (h *hive) registerApp(a *app) {
	h.apps[a.Name()] = a
}

func (h *hive) registerHandler(t MsgType, m *mapper, hdl Handler) {
	h.mappers[t] = append(h.mappers[t], mapperAndHandler{m, hdl})
}

func (h *hive) handleMsg(m *msg) {
	for _, mh := range h.mappers[m.Type()] {
		mh.mapr.dataCh <- msgAndHandler{m, mh.handler}
	}
}

func (h *hive) startMappers() {
	for _, a := range h.apps {
		go a.mapper.start()
	}
}

func (h *hive) Start(joinCh chan interface{}) error {
	defer close(joinCh)

	h.status = HiveStarted
	h.registerSignals()
	h.connectToRegistery()
	h.startMappers()

	for {
		select {
		case msg, ok := <-h.dataCh:
			if !ok {
				return errors.New("Data channel is closed.")
			}
			h.handleMsg(msg)
		case cmd, ok := <-h.ctrlCh:
			if !ok {
				return errors.New("Control channel is closed.")
			}
			h.handleCmd(cmd)
		case <-joinCh:
			glog.Fatalf("Hive'h join channel should not be used nor closed.")
		}
	}
	return nil
}

func (h *hive) Stop() error {
	if h.ctrlCh == nil {
		return errors.New("Control channel is closed.")
	}

	if h.Status() == HiveStopped {
		return errors.New("Hive is already stopped.")
	}

	h.ctrlCh <- StopHive
	return nil
}

func (h *hive) Status() HiveStatus {
	return h.status
}

func (h *hive) NewApp(name AppName) App {
	a := &app{
		name:     name,
		hive:     h,
		handlers: make(map[MsgType]Handler),
	}
	a.initMapper()
	h.registerApp(a)
	return a
}

func (h *hive) Emit(msgData interface{}) {
	h.emitMsg(&msg{MsgData: msgData, MsgType: msgType(msgData)})
}

func (h *hive) emitMsg(msg *msg) {
	switch {
	case msg.isBroadCast():
		h.dataCh <- msg
	case msg.isUnicast():
		a, ok := h.app(msg.To().AppName)
		if !ok {
			glog.Fatalf("Application not found: %h", msg.To().AppName)
		}
		a.mapper.dataCh <- msgAndHandler{msg, a.handler(msg.Type())}
	}
}

func (h *hive) SendToDictKey(msgData interface{}, to AppName,
	dk DictionaryKey) {
	// TODO(soheil): Implement this hive.SendTo.
	glog.Fatalf("Not implemented yet.")
}

func (h *hive) SentToRcvr(msgData interface{}, to RcvrId) {
	h.emitMsg(newMsgFromData(msgData, RcvrId{}, to))
}

// Reply to thatMsg with the provided replyData.
func (h *hive) ReplyTo(thatMsg Msg, replyData interface{}) error {
	m := thatMsg.(*msg)
	if m.NoReply() {
		return errors.New("Cannot reply to this message.")
	}

	h.emitMsg(newMsgFromData(replyData, RcvrId{}, m.From()))
	return nil
}
