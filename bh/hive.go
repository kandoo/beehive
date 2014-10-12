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

type Hive interface {
	// ID of the hive. Valid only if the hive is started.
	ID() HiveID

	// Start starts the hive. This function blocks.
	Start() error
	// Stop stops the hive and all its apps. It blocks until the hive is actually
	// stopped.
	Stop() error

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
	StatePath       string        // Where to store state data.
	DataChBufSize   int           // Buffer size of the data channels.
	CmdChBufSize    int           // Buffer size of the control channels.
	Instrument      bool          // Whether to instrument apps on the hive.
	HBQueryInterval time.Duration // Heartbeating interval.
	HBDeadTimeout   time.Duration // When to announce a bee dead.
	RegLockTimeout  time.Duration // When to retry to lock an entry in a registry.
	UseBeeHeartbeat bool          // Heartbeat bees instead of the registry.
}

// Creates a new hive based on the given configuration.
func NewHiveWithConfig(cfg HiveConfig) Hive {
	h := &hive{
		id:     HiveID(cfg.HiveAddr),
		status: hiveStopped,
		config: cfg,
		dataCh: make(chan *msg, cfg.DataChBufSize),
		ctrlCh: make(chan HiveCmd),
		apps:   make(map[AppName]*app, 0),
		qees:   make(map[MsgType][]qeeAndHandler),
	}

	h.init()

	return h
}

var DefaultCfg = HiveConfig{}

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
	flag.StringVar(&DefaultCfg.StatePath, "statepath", "/tmp",
		"Where to store persistent state data.")
	flag.DurationVar(&DefaultCfg.HBQueryInterval, "hbqueryinterval",
		100*time.Millisecond, "Heartbeat interval.")
	flag.DurationVar(&DefaultCfg.HBDeadTimeout, "hbdeadtimeout",
		300*time.Millisecond,
		"The timeout after which a non-responsive bee is announced dead.")
	flag.DurationVar(&DefaultCfg.RegLockTimeout, "reglocktimeout",
		10*time.Millisecond, "Timeout to retry locking an entry in the registry")
	flag.BoolVar(&DefaultCfg.UseBeeHeartbeat, "userbeehb", false,
		"Whether to use high-granular bee heartbeating in addition to registry"+
			"events")
}

type qeeAndHandler struct {
	q *qee
	h Handler
}

// hiveStatus represents the status of a hive.
type hiveStatus int

// Valid values for HiveStatus.
const (
	hiveStopped hiveStatus = iota
	hiveStarted            = iota
)

// The internal implementation of Hive.
type hive struct {
	id     HiveID
	config HiveConfig

	status hiveStatus

	dataCh chan *msg
	ctrlCh chan HiveCmd
	sigCh  chan os.Signal

	apps map[AppName]*app
	qees map[MsgType][]qeeAndHandler

	registry  registry
	collector statCollector

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
	if h.listener != nil {
		h.listener.Close()
	}

	glog.Info("Stopping qees...")
	qs := make(map[*qee]bool)
	for _, mhs := range h.qees {
		for _, mh := range mhs {
			qs[mh.q] = true
		}
	}

	stopCh := make(chan CmdResult)
	for q, _ := range qs {
		q.ctrlCh <- NewLocalCmd(stopCmd{}, BeeID{}, stopCh)
		glog.V(3).Infof("Waiting on a qee: %v", q.id())
		stopped := false
		tries := 5
		for !stopped {
			select {
			case res := <-stopCh:
				_, err := res.get()
				if err != nil {
					glog.Errorf("Error in stopping a qee: %v", err)
				}
				stopped = true
			case <-time.After(1 * time.Second):
				if tries--; tries < 0 {
					glog.Infof("Giving up on qee %v", q.id())
					stopped = true
					continue
				}
				glog.Infof("Still waiting for a qee %v...", q.id())
			}
		}
	}
}

func (h *hive) init() {
	gob.Register(HiveID(""))
	gob.Register(BeeID{})
	gob.Register(BeeColony{})

	gob.Register(TxSeq(0))
	gob.Register(TxGeneration(0))
	gob.Register(Tx{})
	gob.Register(TxInfo{})

	gob.Register(StateOp{})
	gob.Register(inMemDict{})
	gob.Register(inMemoryState{})

	gob.Register(msg{})

	gob.Register(CmdResult{})
	gob.Register(stopCmd{})
	gob.Register(startCmd{})
	gob.Register(findBeeCmd{})
	gob.Register(createBeeCmd{})
	gob.Register(joinColonyCmd{})
	gob.Register(bufferTxCmd{})
	gob.Register(commitTxCmd{})
	gob.Register(getTxInfoCmd{})
	gob.Register(getTx{})
	gob.Register(migrateBeeCmd{})
	gob.Register(replaceBeeCmd{})
	gob.Register(lockMappedCellsCmd{})
	gob.Register(getColonyCmd{})
	gob.Register(addSlaveCmd{})
	gob.Register(delSlaveCmd{})
	gob.Register(addMappedCells{})

	gob.Register(GobError{})

	if h.config.Instrument {
		h.collector = newAppStatCollector(h)
	} else {
		h.collector = &dummyStatCollector{}
	}
	startHeartbeatHandler(h)
	h.replStrategy = newRndReplication(h)
}

func (h *hive) handleCmd(cmd HiveCmd) {
	switch cmd.Type {
	case StopHive:
		// TODO(soheil): This has a race with Stop(). Use atomics here.
		h.status = hiveStopped
		h.registry.disconnect()
		h.closeChannels()
		cmd.ResCh <- true
	case DrainHive:
		// TODO(soheil): Implement drain.
		glog.Fatalf("Drain Hive is not implemented.")
	case PingHive:
		cmd.ResCh <- true
	}
}

func (h *hive) registerApp(a *app) {
	h.apps[a.Name()] = a
}

func (h *hive) registerHandler(t MsgType, q *qee, l Handler) {
	for i, qh := range h.qees[t] {
		if qh.q == q {
			h.qees[t][i].h = l
			return
		}
	}

	h.qees[t] = append(h.qees[t], qeeAndHandler{q, l})
}

func (h *hive) handleMsg(m *msg) {
	for _, qh := range h.qees[m.Type()] {
		qh.q.dataCh <- msgAndHandler{m, qh.h}
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

func (h *hive) Start() error {
	h.status = hiveStarted
	h.startListener()
	h.registerSignals()
	h.connectToRegistry()
	h.startQees()

	for h.status == hiveStarted {
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
		}
	}

	return nil
}

func (h *hive) Stop() error {
	if h.ctrlCh == nil {
		return errors.New("Control channel is closed.")
	}

	if h.status == hiveStopped {
		return errors.New("Hive is already stopped.")
	}

	resCh := make(chan interface{})
	h.ctrlCh <- HiveCmd{
		Type:  StopHive,
		ResCh: resCh,
	}
	<-resCh
	return nil
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
	a.SetFlags(AppFlagTransactional)

	a.Handle(heartbeatReq{}, &heartbeatReqHandler{})
	mod := &colonyModerator{h.config.RegLockTimeout}
	a.Handle(beeFailed{}, mod)
	a.Handle(HiveJoined{}, mod)
	a.Handle(HiveLeft{}, mod)

	return a
}

func (h *hive) Emit(msgData interface{}) {
	h.emitMsg(&msg{MsgData: msgData, MsgType: msgType(msgData)})
}

func (h *hive) emitMsg(msg *msg) {
	switch {
	case msg.isUnicast():
		a, ok := h.app(msg.MsgTo.AppName)
		if !ok {
			glog.Fatalf("Application not found: %s", msg.To().AppName)
		}
		a.qee.dataCh <- msgAndHandler{msg, a.handler(msg.Type())}
	default:
		h.dataCh <- msg
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
