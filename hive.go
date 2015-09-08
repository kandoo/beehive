package beehive

import (
	"encoding/gob"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	etcdraft "github.com/kandoo/beehive/Godeps/_workspace/src/github.com/coreos/etcd/raft"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/golang/glog"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/soheilhy/args"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/soheilhy/cmux"
	"github.com/kandoo/beehive/Godeps/_workspace/src/golang.org/x/net/context"
	"github.com/kandoo/beehive/raft"
	"github.com/kandoo/beehive/randtime"
)

const (
	hiveGroup = 1
)

// Hive represents is the main active entity of beehive. It mananges all
// messages, apps and bees.
type Hive interface {
	// ID of the hive. Valid only if the hive is started.
	ID() uint64
	// Config returns the hive configuration.
	Config() HiveConfig

	// Start starts the hive. This function blocks.
	Start() error
	// Stop stops the hive and all its apps. It blocks until the hive is actually
	// stopped.
	Stop() error

	// Creates an app with the given name and the provided options.
	// Note that apps are not active until the hive is started.
	NewApp(name string, opts ...AppOption) App

	// Emits a message containing msgData from this hive.
	Emit(msgData interface{})
	// Sends a message to a specific bee that owns a specific dictionary key.
	SendToCellKey(msgData interface{}, to string, dk CellKey)
	// Sends a message to a sepcific bee.
	SendToBee(msgData interface{}, to uint64)
	// Reply replies to the message.
	Reply(msg Msg, replyData interface{}) error
	// Sync processes a synchrounous message (req) and blocks until the response
	// is recieved.
	Sync(ctx context.Context, req interface{}) (res interface{}, err error)

	// Registers a message for encoding/decoding. This method should be called
	// only on messages that have no active handler. Such messages are almost
	// always replies to some detached handler.
	RegisterMsg(msg interface{})
}

// HiveConfig represents the configuration of a hive.
type HiveConfig struct {
	Addr      string   // public address of the hive.
	PeerAddrs []string // peer addresses.
	StatePath string   // where to store state data.

	DataChBufSize uint // buffer size of the data channels.
	CmdChBufSize  uint // buffer size of the control channels.
	BatchSize     uint // number of messages to batch.
	SyncPoolSize  uint // number of sync go-routines.

	Pprof          bool // whether to enable pprof web handlers.
	Instrument     bool // whether to instrument apps on the hive.
	OptimizeThresh uint // when to notify the optimizer (in msg/s).

	RaftTick       time.Duration // the raft tick interval.
	RaftTickDelta  time.Duration // the maximum random delta added to the tick.
	RaftFsyncTick  time.Duration // the frequency of Fsync.
	RaftHBTicks    int           // number of raft ticks that fires a heartbeat.
	RaftElectTicks int           // number of raft ticks that fires election.
	RaftInFlights  int           // maximum number of inflights to a node.
	RaftMaxMsgSize uint64        // maximum size of an append message.

	ConnTimeout time.Duration // timeout for connections between hives.
}

// RaftElectTimeout returns the raft election timeout as
// RaftTick*RaftElectTicks.
func (c HiveConfig) RaftElectTimeout() time.Duration {
	return time.Duration(c.RaftElectTicks) * (c.RaftTick + c.RaftTickDelta)
}

// RaftHBTimeout returns the raft heartbeat timeout as RaftTick*RaftHBTicks.
func (c HiveConfig) RaftHBTimeout() time.Duration {
	return time.Duration(c.RaftHBTicks) * (c.RaftTick + c.RaftTickDelta)
}

var raftLogOnce sync.Once

// HiveOption represents a configuration option of a hive.
type HiveOption args.V

var addr = args.NewString(args.Flag("addr", "localhost:7677",
	"the server listening address used for both RPC and HTTP"))

// Addr represents the listening address of the hive used for both inter-hive
// RPC and its HTTP/web interface.
func Addr(a string) HiveOption { return HiveOption(addr(a)) }

var paddrs = args.NewString(args.Flag("paddrs", "",
	"address of peers. Seperate entries with a comma"))

// PeerAddrs represents the peer addresses of hive.
func PeerAddrs(pa ...string) HiveOption {
	return HiveOption(paddrs(strings.Join(pa, ",")))
}

var dataChBufSize = args.NewUint(args.Flag("chsize", uint(1024),
	"buffer size of data channels"))

// DataChBufSize represents the size of the message channels used by hives,
// queen bees and bees.
func DataChBufSize(s uint) HiveOption { return HiveOption(dataChBufSize(s)) }

var cmdChBufSize = args.NewUint(args.Flag("cmdchsize", uint(128),
	"buffer size of command channels"))

// CmdChBufSize represents the size of the command channel used by hives,
// queen bees and bees.
func CmdChBufSize(s uint) HiveOption { return HiveOption(cmdChBufSize(s)) }

var batchSize = args.NewUint(args.Flag("batch", uint(1024),
	"number of messages to batch per transaction"))

// BatchSize represents the maximum batch size used for batching messages
// in a hive.
func BatchSize(s uint) HiveOption { return HiveOption(batchSize(s)) }

var syncPoolSize = args.NewUint(args.Flag("sync", uint(16),
	"number of sync go-routines"))

// SyncPoolSize represents the number of sync go-routines running in a hive.
// These go-routine handle sync requests.
func SyncPoolSize(s uint) HiveOption { return HiveOption(syncPoolSize(s)) }

var pprof = args.NewBool(args.Flag("pprof", false,
	"whether to install pprof on /debug/pprof"))

// Pprof represents whether the hive should enable pprof handlers on its HTTP
// interface.
func Pprof(p bool) HiveOption { return HiveOption(pprof(p)) }

var instrument = args.NewBool(args.Flag("instrument", false,
	"whether to insturment apps"))

// InstrumentOptimize represents whether the hive should perform runtime
// intstrumentation and optimization.
func InstrumentOptimize(i bool) HiveOption { return HiveOption(instrument(i)) }

var optimizeThresh = args.NewUint(args.Flag("optthresh", uint(10),
	"when the local stat collector should notify the optimizer (in msg/s)."))

// OptimizeThresh represents the minimum message rate (i.e., the number of
// messages per second) after which we notify the optimizer.
func OptimizeThresh(t uint) HiveOption { return HiveOption(optimizeThresh(t)) }

var statePath = args.NewString(args.Flag("statepath", "/tmp/beehive",
	"where to store persistent state data"))

// StatePath represents where the hive should save its state.
func StatePath(p string) HiveOption { return HiveOption(statePath(p)) }

var raftTick = args.NewDuration(args.Flag("rafttick", 100*time.Millisecond,
	"raft tick period"))

// RaftTick represents the raft tick.
func RaftTick(t time.Duration) HiveOption { return HiveOption(raftTick(t)) }

var raftTickDelta = args.NewDuration(args.Flag("deltarafttick",
	0*time.Millisecond, "max random duration added to the raft tick per tick"))

// RaftTickDelta represents the random tick to add to the main raft tick.
func RaftTickDelta(d time.Duration) HiveOption {
	return HiveOption(raftTickDelta(d))
}

var raftFsyncTick = args.NewDuration(args.Flag("raftfsync", 1*time.Second,
	"the frequency of raft fsync. 0 means always sync immidiately"))

// RaftFsyncTick represents when the hive should call fsync on written entires.
// 0 means immidiately after each write.
func RaftFsyncTick(t time.Duration) HiveOption {
	return HiveOption(raftFsyncTick(t))
}

var raftElectTicks = args.NewInt(args.Flag("raftelectionticks", 5,
	"number of raft ticks to start an election (ie, election timeout)"))

// RaftElectTicks represents the number of ticks to start a new raft election.
func RaftElectTicks(e int) HiveOption { return HiveOption(raftElectTicks(e)) }

var raftHbeatTicks = args.NewInt(args.Flag("rafthbticks", 1,
	"number of raft ticks to fire a heartbeat (ie, heartbeat timeout)"))

// RaftHbeatTicks represents the number of ticks to send a new raft heartbeat.
func RaftHbeatTicks(h int) HiveOption { return HiveOption(raftHbeatTicks(h)) }

// TODO(soheil): use a better set of default values.
var raftInFlights = args.NewInt(args.Flag("raftmaxinflights", 4096/8,
	"maximum number of inflight raft append messages"))

// RaftInFlights represents the maximum number of raft messages in flight.
func RaftInFlights(f int) HiveOption { return HiveOption(raftInFlights(f)) }

var raftMaxMsgSize = args.NewUint64(args.Flag("raftmaxmsgsize",
	uint64(1*1024*1024), "maximum number of a raft append message"))

// RaftMaxMsgSize represents the maximum number of entries in a raft message.
func RaftMaxMsgSize(s uint64) HiveOption {
	return HiveOption(raftMaxMsgSize(s))
}

var connTimeout = args.NewDuration(args.Flag("conntimeout", 60*time.Second,
	"timeout for trying to connect to other hives"))

// ConnTimeout represents the connection timeout for RPC connections.
func ConnTimeout(t time.Duration) HiveOption {
	return HiveOption(connTimeout(t))
}

func hiveConfig(opts ...HiveOption) (cfg HiveConfig) {
	cfg.Addr = addr.Get(opts)
	if pa := paddrs.Get(opts); pa != "" {
		cfg.PeerAddrs = strings.Split(pa, ",")
	}
	cfg.StatePath = statePath.Get(opts)
	cfg.DataChBufSize = dataChBufSize.Get(opts)
	cfg.CmdChBufSize = cmdChBufSize.Get(opts)
	cfg.BatchSize = batchSize.Get(opts)
	cfg.SyncPoolSize = syncPoolSize.Get(opts)
	cfg.Pprof = pprof.Get(opts)
	cfg.Instrument = instrument.Get(opts)
	cfg.OptimizeThresh = optimizeThresh.Get(opts)
	cfg.RaftTick = raftTick.Get(opts)
	cfg.RaftTickDelta = raftTickDelta.Get(opts)
	cfg.RaftFsyncTick = raftFsyncTick.Get(opts)
	cfg.RaftHBTicks = raftHbeatTicks.Get(opts)
	cfg.RaftElectTicks = raftElectTicks.Get(opts)
	cfg.RaftInFlights = raftInFlights.Get(opts)
	cfg.RaftMaxMsgSize = raftMaxMsgSize.Get(opts)
	cfg.ConnTimeout = connTimeout.Get(opts)
	return cfg
}

// NewHive creates a new hive based on the given configuration options.
func NewHive(opts ...HiveOption) Hive {
	if !flag.Parsed() {
		flag.Parse()
	}

	if !glog.V(1) {
		raftLogOnce.Do(func() {
			etcdraft.SetLogger(&etcdraft.DefaultLogger{
				Logger: log.New(ioutil.Discard, "", 0),
			})
		})
	}

	cfg := hiveConfig(opts...)
	os.MkdirAll(cfg.StatePath, 0700)
	m := meta(cfg)
	h := &hive{
		id:     m.Hive.ID,
		meta:   m,
		status: hiveStopped,
		config: cfg,
		dataCh: newMsgChannel(cfg.DataChBufSize),
		ctrlCh: make(chan cmdAndChannel),
		syncCh: make(chan syncReqAndChan, cfg.DataChBufSize),
		apps:   make(map[string]*app, 0),
		qees:   make(map[string][]qeeAndHandler),
	}

	h.client = newRPCClientPool(h)
	h.registry = newRegistry(h.String())
	h.replStrategy = newRndReplication(h)
	h.httpServer = newServer(h)

	if h.config.Instrument {
		h.collector = newAppStatCollector(h)
	} else {
		h.collector = &noOpStatCollector{}
	}

	h.initSync()

	return h
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
	hiveStarted
)

// The internal implementation of Hive.
type hive struct {
	sync.Mutex

	id     uint64
	meta   hiveMeta
	config HiveConfig

	status hiveStatus

	dataCh *msgChannel
	ctrlCh chan cmdAndChannel
	syncCh chan syncReqAndChan
	sigCh  chan os.Signal

	apps map[string]*app
	qees map[string][]qeeAndHandler

	httpServer *httpServer
	listener   net.Listener

	node     *raft.MultiNode
	registry *registry
	ticker   *randtime.Ticker
	client   *rpcClientPool

	replStrategy replicationStrategy
	collector    collector
}

func (h *hive) ID() uint64 {
	return h.id
}

func (h *hive) String() string {
	return fmt.Sprintf("hive %v@%v", h.id, h.config.Addr)
}

func (h *hive) Config() HiveConfig {
	return h.config
}

func (h *hive) RegisterMsg(msg interface{}) {
	gob.Register(msg)
}

// Sync processes a synchrounous request and returns the response and error.
func (h *hive) Sync(ctx context.Context, req interface{}) (res interface{},
	err error) {

	id := uint64(rand.Int63())
	ch := make(chan syncRes, 1)

	// We should run this in parallel in case we are blocked on h.syncCh.
	go func() {
		sc := syncReqAndChan{
			req: syncReq{ID: id, Data: req},
			ch:  ch,
		}
		select {
		case h.syncCh <- sc:
		case <-ctx.Done():
		}
	}()

	select {
	case r := <-ch:
		if r.Err != nil {
			return nil, errors.New(r.Err.Error())
		}
		return r.Data, nil

	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func (h *hive) app(name string) (*app, bool) {
	a, ok := h.apps[name]
	return a, ok
}

func (h *hive) hiveAddr(id uint64) (string, error) {
	i, err := h.registry.hive(id)
	return i.Addr, err
}

func (h *hive) stopListener() {
	glog.Infof("%v closes listener...", h)
	if h.listener != nil {
		h.listener.Close()
	}
}

func (h *hive) stopQees() {
	glog.Infof("%v is stopping qees...", h)
	qs := make(map[*qee]bool)
	for _, mhs := range h.qees {
		for _, mh := range mhs {
			qs[mh.q] = true
		}
	}

	stopCh := make(chan cmdResult)
	for q := range qs {
		q.ctrlCh <- newCmdAndChannel(cmdStop{}, h.ID(), q.app.Name(), 0, stopCh)
		glog.V(3).Infof("waiting on a qee: %v", q)
		stopped := false
		tries := 5
		for !stopped {
			select {
			case res := <-stopCh:
				_, err := res.get()
				if err != nil {
					glog.Errorf("error in stopping a qee: %v", err)
				}
				stopped = true
			case <-time.After(1 * time.Second):
				if tries--; tries < 0 {
					glog.Infof("giving up on qee %v", q)
					stopped = true
					continue
				}
				glog.Infof("still waiting for a qee %v...", q)
			}
		}
	}
}

func (h *hive) handleCmd(cc cmdAndChannel) {
	glog.V(2).Infof("%v handles cmd %+v", h, cc.cmd)
	switch d := cc.cmd.Data.(type) {
	case cmdStop:
		// TODO(soheil): This has a race with Stop(). Use atomics here.
		h.status = hiveStopped
		h.stopListener()
		h.stopQees()
		h.node.Stop()
		cc.ch <- cmdResult{}

	case cmdPing:
		cc.ch <- cmdResult{}

	case cmdSync:
		err := h.raftBarrier()
		cc.ch <- cmdResult{Err: err}

	case cmdNewHiveID:
		r, err := h.node.ProposeRetry(hiveGroup, newHiveID{},
			h.config.RaftElectTimeout(), 10)
		cc.ch <- cmdResult{
			Data: r,
			Err:  err,
		}

	case cmdAddHive:
		err := h.node.AddNodeToGroup(context.TODO(), d.Hive.ID, hiveGroup,
			d.Hive.Addr)
		cc.ch <- cmdResult{
			Err: err,
		}

	case cmdLiveHives:
		cc.ch <- cmdResult{
			Data: h.registry.hives(),
		}

	default:
		cc.ch <- cmdResult{
			Err: ErrInvalidCmd,
		}
	}
}

func (h *hive) processCmd(data interface{}) (interface{}, error) {
	ch := make(chan cmdResult)
	h.ctrlCh <- newCmdAndChannel(data, h.ID(), "", 0, ch)
	return (<-ch).get()
}

func (h *hive) raftBarrier() error {
	// TODO(soheil): maybe add a max retry number into the configs.
	_, err := h.node.ProposeRetry(hiveGroup, noOp{},
		10*h.config.RaftElectTimeout(), -1)
	return err
}

func (h *hive) registerApp(a *app) {
	h.apps[a.Name()] = a
}

func (h *hive) registerHandler(t string, q *qee, l Handler) {
	for i, qh := range h.qees[t] {
		if qh.q == q {
			h.qees[t][i].h = l
			return
		}
	}

	h.qees[t] = append(h.qees[t], qeeAndHandler{q, l})
}

func (h *hive) initSync() {
	a := h.NewApp("beehive-sync")
	for i := uint(0); i < h.config.SyncPoolSize; i++ {
		newSync(a, h.syncCh)
	}
}

func (h *hive) bee(id uint64) (BeeInfo, error) {
	return h.registry.bee(id)
}

func (h *hive) handleMsg(m *msg) {
	switch {
	case m.IsUnicast():
		i, err := h.bee(m.MsgTo)
		if err != nil {
			glog.Errorf("no such bee %v", m.MsgTo)
			return
		}
		a, ok := h.app(i.App)
		if !ok {
			glog.Fatalf("no such application %s", i.App)
		}
		if i.Detached {
			a.qee.enqueMsg(msgAndHandler{msg: m})
			return
		}
		a.qee.enqueMsg(msgAndHandler{msg: m, handler: a.handler(m.Type())})
	default:
		for _, qh := range h.qees[m.Type()] {
			qh.q.enqueMsg(msgAndHandler{m, qh.h})
		}
	}
}

func (h *hive) startQees() {
	for _, a := range h.apps {
		go a.qee.start()
	}
}

func (h *hive) startRaftNode() {
	peers := make([]etcdraft.Peer, 0, 1)
	if len(h.meta.Peers) != 0 {
		h.registry.initHives(h.meta.Peers)
	} else {
		i := h.info()
		ni := raft.GroupNode{
			Group: hiveGroup,
			Node:  i.ID,
			Data:  i.Addr,
		}
		peers = append(peers, ni.Peer())
	}

	h.ticker = randtime.NewTicker(h.config.RaftTick, h.config.RaftTickDelta)

	ncfg := raft.Config{
		ID:     h.id,
		Name:   h.String(),
		Send:   h.sendRaft,
		Ticker: h.ticker.C,
	}
	h.node = raft.StartMultiNode(ncfg)

	gcfg := raft.GroupConfig{
		ID:             hiveGroup,
		Name:           h.String(),
		StateMachine:   h.registry,
		Peers:          peers,
		DataDir:        h.config.StatePath,
		SnapCount:      1024,
		FsyncTick:      h.config.RaftFsyncTick,
		ElectionTicks:  h.config.RaftElectTicks,
		HeartbeatTicks: h.config.RaftHBTicks,
		MaxInFlights:   h.config.RaftInFlights,
		MaxMsgSize:     h.config.RaftMaxMsgSize,
	}
	if err := h.node.CreateGroup(context.TODO(), gcfg); err != nil {
		glog.Fatalf("cannot create hive group: %v", err)
	}
}

func (h *hive) proposeAmongHives(ctx context.Context, req interface{}) (
	res interface{}, err error) {

	return h.node.Propose(ctx, hiveGroup, req)
}

func (h *hive) delBeeFromRegistry(id uint64) error {
	_, err := h.node.ProposeRetry(hiveGroup, delBee(id),
		h.config.RaftElectTimeout(), -1)
	if err == ErrNoSuchBee {
		err = nil
	}
	if err != nil {
		glog.Errorf("%v cannot delete bee %v from registory: %v", h, id, err)
	}
	return err
}

func (h *hive) reloadState() {
	for _, b := range h.registry.beesOfHive(h.id) {
		if b.Detached || b.Colony.IsNil() {
			glog.V(1).Infof(
				"%v will not reload detached bee %v (detached=%v, colony=%#v)", h, b.ID,
				b.Detached, b.Colony)
			go h.delBeeFromRegistry(b.ID)
			continue
		}
		a, ok := h.app(b.App)
		if !ok {
			glog.Errorf("app %v is not registered but has a bee", b.App)
			continue
		}
		_, err := a.qee.processCmd(cmdReloadBee{ID: b.ID, Colony: b.Colony})
		if err != nil {
			glog.Errorf("cannot reload bee %v on %v", b.ID, h.id)
			continue
		}
	}
}

func (h *hive) Start() error {
	h.status = hiveStarted
	h.registerSignals()
	h.startRaftNode()
	if err := h.listen(); err != nil {
		glog.Errorf("%v cannot start listener: %v", h, err)
		h.Stop()
		return err
	}
	if err := h.raftBarrier(); err != nil {
		glog.Fatalf("error when joining the cluster: %v", err)
	}
	glog.V(2).Infof("%v is in sync with the cluster", h)
	h.startQees()
	h.reloadState()

	glog.V(2).Infof("%v starts message loop", h)
	dataCh := h.dataCh.out()
	for h.status == hiveStarted {
		select {
		case m := <-dataCh:
			h.handleMsg(m.msg)

		case cmd := <-h.ctrlCh:
			h.handleCmd(cmd)
		}
	}
	return nil
}

func (h *hive) info() HiveInfo {
	return HiveInfo{
		ID:   h.id,
		Addr: h.config.Addr,
	}
}

func (h *hive) Stop() error {
	glog.Infof("stopping %v", h)
	if h.ctrlCh == nil {
		return errors.New("control channel is closed")
	}

	if h.status == hiveStopped {
		return errors.New("hive is already stopped")
	}

	_, err := h.processCmd(cmdStop{})
	return err
}

func (h *hive) waitUntilStarted() {
	h.processCmd(cmdPing{})
}

func (h *hive) NewApp(name string, options ...AppOption) App {
	a := &app{
		name:     name,
		hive:     h,
		handlers: make(map[string]Handler),
	}
	a.initQee()
	h.registerApp(a)

	if len(options) == 0 {
		options = defaultAppOptions
	}

	for _, opt := range options {
		opt(a)
	}

	return a
}

func (h *hive) Emit(msgData interface{}) {
	h.enqueMsg(&msg{MsgData: msgData})
}

func (h *hive) enqueMsg(msg *msg) {
	h.dataCh.in() <- msgAndHandler{msg: msg}
}

func (h *hive) SendToCellKey(msgData interface{}, to string, k CellKey) {
	// TODO(soheil): Implement this hive.SendTo.
	glog.Fatalf("FIXME implement SendToCellKey")
}

func (h *hive) SendToBee(msgData interface{}, to uint64) {
	h.enqueMsg(newMsgFromData(msgData, 0, to))
}

// Reply to thatMsg with the provided replyData.
func (h *hive) Reply(thatMsg Msg, replyData interface{}) error {
	m := thatMsg.(*msg)
	if m.NoReply() {
		return errors.New("cannot reply to this message")
	}

	h.enqueMsg(newMsgFromData(replyData, 0, m.From()))
	return nil
}

func (h *hive) registerSignals() {
	h.sigCh = make(chan os.Signal, 1)
	signal.Notify(h.sigCh,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		<-h.sigCh
		h.Stop()
	}()
}

func (h *hive) listen() (err error) {
	h.listener, err = net.Listen("tcp", h.config.Addr)
	if err != nil {
		glog.Errorf("%v cannot listen: %v", h, err)
		return err
	}
	glog.Infof("%v is listening", h)

	m := cmux.New(h.listener)
	hl := m.Match(cmux.HTTP1Fast())
	rl := m.Match(cmux.Any())

	go func() {
		h.httpServer.Serve(hl)
		glog.Infof("%v closed http listener", h)
	}()

	rs := rpc.NewServer()
	if err := rs.RegisterName("rpcServer", newRPCServer(h)); err != nil {
		glog.Fatalf("cannot register rpc server: %v", err)
	}

	go func() {
		for {
			conn, err := rl.Accept()
			if err != nil {
				glog.Infof("%v closed rpc listener", h)
				return
			}
			go rs.ServeConn(conn)
		}
	}()

	go m.Serve()

	return nil
}

func (h *hive) sendRaft(batch *raft.Batch, r raft.Reporter) {
	go func() {
		if err := h.client.sendRaft(batch, r); err != nil &&
			!isBackoffError(err) {

			glog.Errorf("%v cannot send raft messages: %v", h, err)
		}
	}()
}
