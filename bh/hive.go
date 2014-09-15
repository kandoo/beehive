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

// HiveID represents the globally unique ID of a hive. These IDs are
// automatically assigned using the distributed configuration service.
type HiveID string

// HiveStatus represents the status of a hive.
type HiveStatus int

// Valid values for HiveStatus.
const (
	HiveStopped HiveStatus = iota
	HiveStarted            = iota
)

type Hive interface {
	// ID of the hive. Valid only if the hive is started.
	ID() HiveID

	// Starts the hive and will close the waitCh once the hive stops.
	Start(joinCh chan bool) error
	// Stops the hive and all its apps.
	Stop() error
	// Returns the hive status.
	Status() HiveStatus

	// Creates an app with the given name. Note that apps are not active until
	// the hive is started.
	NewApp(name AppName) App

	// Emits a message containing msgData from this hive.
	Emit(msgData interface{})
	// Sends a message to a specific bee that owns a specific dictionary key.
	SendToCellKey(msgData interface{}, to AppName, dk CellKey)
	// Sends a message to a sepcific bee.
	SendToBee(msgData interface{}, to BeeID)
	// Replies to a message.
	ReplyTo(msg Msg, replyData interface{}) error

	// Registers a message for encoding/decoding. This method should be called
	// only on messages that have no active handler. Such messages are almost
	// always replies to some detached handler.
	RegisterMsg(msg interface{})

	// ReplicationStrategy returns the registered replication strategy for this
	// hive.
	ReplicationStrategy() ReplicationStrategy
}

// Configuration of a hive.
type HiveConfig struct {
	HiveAddr        string        // Listening address of the hive.
	RegAddrs        []string      // Reigstery service addresses.
	DataChBufSize   int           // Buffer size of the data channels.
	CmdChBufSize    int           // Buffer size of the control channels.
	Instrument      bool          // Whether to instrument apps on the hive.
	DBDir           string        // Directory to persist application state.
	HBQueryInterval time.Duration // Heartbeating interval.
	HBDeadTimeout   time.Duration // When to announce a bee dead.
}

// Creates a new hive based on the given configuration.
func NewHiveWithConfig(cfg HiveConfig) Hive {
	h := &hive{
		id:     HiveID(cfg.HiveAddr),
		status: HiveStopped,
		config: cfg,
		dataCh: make(chan *msg, cfg.DataChBufSize),
		ctrlCh: make(chan HiveCmd),
		apps:   make(map[AppName]*app, 0),
		qees:   make(map[MsgType][]qeeAndHandler),
	}

	h.init()

	return h
}

func (h *hive) init() {
	gob.Register(BeeID{})
	gob.Register(HiveID(""))

	gob.Register(inMemDict{})
	gob.Register(inMemoryState{})

	gob.Register(StateOp{})
	gob.Register(Tx{})
	gob.Register(TxSeq(0))

	gob.Register(msg{})
	gob.Register(CmdResult{})
	gob.Register(migrateBeeCmdData{})
	gob.Register(replaceBeeCmdData{})
	gob.Register(lockMappedCellsData{})

	gob.Register(GobError{})

	if h.config.Instrument {
		h.collector = newAppStatCollector(h)
	} else {
		h.collector = &dummyStatCollector{}
	}

	startHeartbeatHandler(h)
	h.stateMan = newPersistentStateManager(h)

	h.replStrategy = newRndRepl(h)
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

type HiveCmd struct {
	Type  HiveCmdType
	ResCh chan interface{}
}

type HiveCmdType int

const (
	StopHive  HiveCmdType = iota
	DrainHive             = iota
	PingHive              = iota
)

func init() {
	flag.StringVar(&DefaultCfg.HiveAddr, "laddr", "localhost:7767",
		"The listening address used to communicate with other nodes.")
	flag.Var(&commaSeparatedValue{&DefaultCfg.RegAddrs}, "raddrs",
		"Address of etcd machines. Separate entries with a semi-colon ';'")
	flag.IntVar(&DefaultCfg.DataChBufSize, "chsize", 1024,
		"Buffer size of data channels.")
	flag.IntVar(&DefaultCfg.CmdChBufSize, "cmdchsize", 128,
		"Buffer size of command channels.")
	flag.BoolVar(&DefaultCfg.Instrument, "instrument", false,
		"Whether to insturment apps.")
	flag.StringVar(&DefaultCfg.DBDir, "dbdir", "/tmp",
		"Where to store persistent state data.")
	flag.DurationVar(&DefaultCfg.HBQueryInterval, "hbqueryinterval",
		1*time.Second, "Heartbeat interval.")
	flag.DurationVar(&DefaultCfg.HBDeadTimeout, "hbdeadtimeout", 15*time.Second,
		"The timeout after which a non-responsive bee is announced dead.")
}

type qeeAndHandler struct {
	q       *qee
	handler Handler
}

// The internal implementation of Hive.
type hive struct {
	id     HiveID
	status HiveStatus
	config HiveConfig

	dataCh chan *msg
	ctrlCh chan HiveCmd
	sigCh  chan os.Signal

	apps map[AppName]*app
	qees map[MsgType][]qeeAndHandler

	registry  registry
	collector statCollector
	stateMan  *persistentStateManager

	listener net.Listener

	replStrategy ReplicationStrategy
}

func (h *hive) ID() HiveID {
	return h.id
}

func (h *hive) RegisterMsg(msg interface{}) {
	gob.Register(msg)
}

func (h *hive) isolated() bool {
	return !h.registry.connected()
}

func (h *hive) app(name AppName) (*app, bool) {
	a, ok := h.apps[name]
	return a, ok
}

func (h *hive) closeChannels() {
	glog.Info("Closing the hive listener...")
	h.listener.Close()

	glog.Info("Stopping qees...")
	stopCh := make(chan CmdResult)
	qs := make(map[*qee]bool)
	for _, mhs := range h.qees {
		for _, mh := range mhs {
			qs[mh.q] = true
		}
	}

	for m, _ := range qs {
		m.ctrlCh <- NewLocalCmd(stopCmd, nil, BeeID{}, stopCh)
		glog.V(3).Infof("Waiting on a qee: %p", m)
		select {
		case res := <-stopCh:
			_, err := res.get()
			if err != nil {
				glog.Errorf("Error in stopping a qee: %v", err)
			}
		case <-time.After(time.Second * 1):
			glog.Info("Still waiting for a qee...")
		}
	}
	close(h.dataCh)
	close(h.ctrlCh)
	close(h.sigCh)
}

type step int

const (
	stop step = iota
	next      = iota
)

func (h *hive) handleCmd(cmd HiveCmd) step {
	switch cmd.Type {
	case StopHive:
		// TODO(soheil): This has a race with Stop(). Use atomics here.
		h.status = HiveStopped
		h.registry.disconnect()
		h.closeChannels()
		h.stateMan.closeDBs()
		return stop
	case DrainHive:
		// TODO(soheil): Implement drain.
		glog.Fatalf("Drain Hive is not implemented.")
	case PingHive:
		cmd.ResCh <- true
	}

	return next
}

func (h *hive) registerApp(a *app) {
	h.apps[a.Name()] = a
}

func (h *hive) registerHandler(t MsgType, m *qee, hdl Handler) {
	h.qees[t] = append(h.qees[t], qeeAndHandler{m, hdl})
}

func (h *hive) handleMsg(m *msg) {
	for _, mh := range h.qees[m.Type()] {
		mh.q.dataCh <- msgAndHandler{m, mh.handler}
	}
}

func (h *hive) startQees() {
	for _, a := range h.apps {
		go a.qee.start()
	}
}

func (h *hive) startListener() {
	h.listen()
}

func (h *hive) Start(joinCh chan bool) error {
	defer close(joinCh)

	h.status = HiveStarted
	h.startListener()
	h.registerSignals()
	h.connectToRegistry()
	h.startQees()

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

			if h.handleCmd(cmd) == stop {
				return nil
			}
		case <-joinCh:
			glog.Fatalf("Hive'h join channel should not be used nor closed.")
		}
	}
}

func (h *hive) Stop() error {
	if h.ctrlCh == nil {
		return errors.New("Control channel is closed.")
	}

	if h.Status() == HiveStopped {
		return errors.New("Hive is already stopped.")
	}

	h.ctrlCh <- HiveCmd{Type: StopHive}
	return nil
}

func (h *hive) Status() HiveStatus {
	return h.status
}

func (h *hive) waitUntilStarted() {
	ch := make(chan interface{})
	h.ctrlCh <- HiveCmd{
		Type:  PingHive,
		ResCh: ch,
	}
	<-ch
}

func (h *hive) NewApp(name AppName) App {
	a := &app{
		name:     name,
		hive:     h,
		handlers: make(map[MsgType]Handler),
	}
	a.initQee()
	h.registerApp(a)
	a.Handle(heartbeatReq{}, &heartbeatReqHandler{})
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
			glog.Fatalf("Application not found: %s", msg.To().AppName)
		}
		a.qee.dataCh <- msgAndHandler{msg, a.handler(msg.Type())}
	}
}

func (h *hive) SendToCellKey(msgData interface{}, to AppName,
	dk CellKey) {
	// TODO(soheil): Implement this hive.SendTo.
	glog.Fatalf("Not implemented yet.")
}

func (h *hive) SendToBee(msgData interface{}, to BeeID) {
	h.emitMsg(newMsgFromData(msgData, BeeID{}, to))
}

// Reply to thatMsg with the provided replyData.
func (h *hive) ReplyTo(thatMsg Msg, replyData interface{}) error {
	m := thatMsg.(*msg)
	if m.NoReply() {
		return errors.New("Cannot reply to this message.")
	}

	h.emitMsg(newMsgFromData(replyData, BeeID{}, m.From()))
	return nil
}

func (h *hive) ReplicationStrategy() ReplicationStrategy {
	return h.replStrategy
}
