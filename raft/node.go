package raft

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/coreos/etcd/pkg/pbutil"
	etcdraft "github.com/kandoo/beehive/Godeps/_workspace/src/github.com/coreos/etcd/raft"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/coreos/etcd/raft/raftpb"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/golang/glog"
	"github.com/kandoo/beehive/Godeps/_workspace/src/golang.org/x/net/context"
	"github.com/kandoo/beehive/gen"
	bhgob "github.com/kandoo/beehive/gob"
)

// Most of this code is adapted from etcd/etcdserver/server.go.

const (
	numberOfCatchUpEntries = 5000
)

var (
	// ErrStopped is returned when the node is already stopped.
	ErrStopped = errors.New("raft: node stopped")
	// ErrUnreachable is returned when SendFunc cannot reach the destination node.
	ErrUnreachable = errors.New("raft: node unreachable")
	// ErrGroupExists is returned when the group already exists.
	ErrGroupExists = errors.New("raft: group exists")
	// ErrNoSuchGroup is returned when the requested group does not exist.
	ErrNoSuchGroup = errors.New("raft: no such group")
)

type Reporter interface {
	// Report reports the given node is not reachable for the last send.
	ReportUnreachable(id, group uint64)
	// ReportSnapshot reports the stutus of the sent snapshot.
	ReportSnapshot(id, group uint64, status etcdraft.SnapshotStatus)
}

// Priority represents the priority of a message sent
type Priority int

const (
	Low Priority = iota
	Normal
	High
)

// Batch represents a batch of messages from one node to another with
// a specific priority.
type Batch struct {
	From     uint64                      // Source node.
	To       uint64                      // Destination node.
	Priority Priority                    // Priority of this batch.
	Messages map[uint64][]raftpb.Message // List of messages of each group.
}

// SendFunc sent a batch of messages to a group.
//
// The sender should send messages in a way that messages of a higher priority
// are not blocked by messages of a lower priority on network channels.
type SendFunc func(batch *Batch, reporter Reporter)

// GroupNode stores the ID, the group and an application-defined data
// for a node in a raft group.
type GroupNode struct {
	Group uint64      // Group's ID.
	Node  uint64      // Node's ID.
	Data  interface{} // Data is application-defined. Must be registered in gob.
}

// Peer returns a peer which stores the binary representation of the hive info
// in the the peer's context.
func (i GroupNode) Peer() etcdraft.Peer {
	if i.Group == 0 || i.Node == 0 {
		glog.Fatalf("zero group")
	}
	return etcdraft.Peer{
		ID:      i.Node,
		Context: i.MustEncode(),
	}
}

// MustEncode encodes the hive into bytes.
func (i GroupNode) MustEncode() []byte {
	b, err := bhgob.Encode(i)
	if err != nil {
		glog.Fatalf("error in encoding peer: %v", err)
	}
	return b
}

type readySaved struct {
	ready etcdraft.Ready
	saved chan struct{}
}

type group struct {
	node *MultiNode

	id           uint64
	name         string
	stateMachine StateMachine
	raftStorage  *etcdraft.MemoryStorage
	diskStorage  DiskStorage
	syncTime     time.Duration
	snapCount    uint64

	leader    uint64
	confState raftpb.ConfState

	savec   chan readySaved
	saved   uint64
	applyc  chan etcdraft.Ready
	applied uint64
	snapmu  sync.RWMutex
	snapped uint64

	stopc       chan struct{}
	saverDone   chan struct{}
	applierDone chan struct{}
}

func (g *group) String() string {
	return fmt.Sprintf("group %v (%v)", g.id, g.name)
}

func (g *group) startSaver() {
	defer func() {
		if err := g.diskStorage.Close(); err != nil {
			glog.Errorf("%v cannot close disk storage: %v", g, err)
		}
		close(g.saverDone)
	}()
	var sync <-chan time.Time
	for {
		select {
		case rdsv := <-g.savec:
			if err := g.save(rdsv); err != nil {
				if err != ErrStopped {
					glog.Errorf("%v cannot apply entries: %v", g, err)
				}
				return
			}
			if g.syncTime == 0 {
				if err := g.sync(); err != nil {
					return
				}
			} else if sync == nil {
				sync = time.After(g.syncTime)
			}

		case <-sync:
			if err := g.sync(); err != nil {
				return
			}
			sync = nil

		case <-g.node.done:
			return

		case <-g.stopc:
			return
		}
	}
}

func (g *group) sync() error {
	glog.V(2).Infof("%v syncing disk storage", g)
	if err := g.diskStorage.Sync(); err != nil {
		glog.Errorf("%v cannot sync disk storage: %v", g, err)
		return err
	}
	return nil
}

func (g *group) startApplier() {
	defer close(g.applierDone)
	for {
		select {
		case rd := <-g.applyc:
			if err := g.apply(rd); err != nil {
				if err != ErrStopped {
					glog.Errorf("%v cannot apply entries: %v", g, err)
				}
				return
			}

		case <-g.node.done:
			return

		case <-g.stopc:
			return
		}
	}
}

func (g *group) stop() {
	select {
	case g.stopc <- struct{}{}:
	case <-g.saverDone:
	case <-g.applierDone:
	}
	<-g.saverDone
	<-g.applierDone
}

func (g *group) save(rdsv readySaved) error {
	glog.V(3).Infof("%v saving state", g)
	if rdsv.ready.SoftState != nil && rdsv.ready.SoftState.Lead != 0 {
		g.node.notifyElection(g.id)
	}

	// Apply snapshot to storage if it is more updated than current snapped.
	if !etcdraft.IsEmptySnap(rdsv.ready.Snapshot) {
		if err := g.diskStorage.SaveSnap(rdsv.ready.Snapshot); err != nil {
			glog.Fatalf("err in save snapshot: %v", err)
		}
		g.raftStorage.ApplySnapshot(rdsv.ready.Snapshot)
		glog.Infof("%v saved incoming snapshot at index %d", g,
			rdsv.ready.Snapshot.Metadata.Index)
	}

	err := g.diskStorage.Save(rdsv.ready.HardState, rdsv.ready.Entries)
	if err != nil {
		glog.Fatalf("err in raft storage save: %v", err)
	}
	glog.V(3).Infof("%v saved state on disk", g)

	g.raftStorage.Append(rdsv.ready.Entries)
	glog.V(3).Infof("%v appended entries in storage", g)

	// Apply config changes in the node as soon as possible
	// before applying other entries in the state machine.
	for _, e := range rdsv.ready.CommittedEntries {
		if e.Type != raftpb.EntryConfChange {
			continue
		}
		if e.Index <= g.saved {
			continue
		}
		g.saved = e.Index

		var cc raftpb.ConfChange
		pbutil.MustUnmarshal(&cc, e.Data)
		if glog.V(2) {
			glog.Infof("%v applies conf change %s: %s",
				g, formatConfChange(cc), formatEntry(e))
		}

		if err := g.validConfChange(cc); err != nil {
			glog.Errorf("%v received an invalid conf change for node %v: %v",
				g, cc.NodeID, err)
			cc.NodeID = etcdraft.None
			g.node.node.ApplyConfChange(g.id, cc)
			continue
		}

		cch := make(chan struct{})
		go func() {
			g.confState = *g.node.node.ApplyConfChange(g.id, cc)
			close(cch)
		}()

		select {
		case <-g.node.done:
			return ErrStopped
		case <-cch:
		}
	}

	glog.V(3).Infof("%v successfully saved ready", g)
	rdsv.saved <- struct{}{}

	select {
	case g.applyc <- rdsv.ready:
	case <-g.node.done:
	}

	return nil
}

func (g *group) apply(ready etcdraft.Ready) error {
	if ready.SoftState != nil {
		newLead := ready.SoftState.Lead
		if g.leader != newLead {
			g.stateMachine.ProcessStatusChange(LeaderChanged{
				Old:  g.leader,
				New:  newLead,
				Term: ready.HardState.Term,
			})
			g.leader = newLead
		}
	}

	// Recover from snapshot if it is more recent than the currently
	// applied.
	if !etcdraft.IsEmptySnap(ready.Snapshot) &&
		ready.Snapshot.Metadata.Index > g.applied {

		if err := g.stateMachine.Restore(ready.Snapshot.Data); err != nil {
			glog.Fatalf("error in recovering the state machine: %v", err)
		}
		// FIXME(soheil): update the nodes and notify the application?
		g.applied = ready.Snapshot.Metadata.Index
		glog.Infof("%v recovered from incoming snapshot at index %d", g.node,
			g.snapped)
	}

	es := ready.CommittedEntries
	if len(es) == 0 {
		return nil
	}

	firsti := es[0].Index
	if firsti > g.applied+1 {
		glog.Fatalf(
			"1st index of committed entry[%d] should <= applied[%d] + 1",
			firsti, g.applied)
	}

	if glog.V(3) {
		glog.Infof("%v receives raft update: committed=%s appended=%s", g,
			formatEntries(es), formatEntries(ready.Entries))
	}

	for _, e := range es {
		if e.Index <= g.applied {
			continue
		}

		switch e.Type {
		case raftpb.EntryNormal:
			if err := g.applyEntry(e); err != nil {
				return err
			}

		case raftpb.EntryConfChange:
			if err := g.applyConfChange(e); err != nil {
				return err
			}

		default:
			glog.Fatalf("unexpected entry type")
		}

		g.applied = e.Index
	}

	if g.applied-g.snapped > g.snapCount {
		glog.Infof("%v start to snapshot (applied: %d, lastsnap: %d)", g,
			g.applied, g.snapped)
		g.snapshot()
	}
	return nil
}

func (g *group) applyEntry(e raftpb.Entry) error {
	glog.V(3).Infof("%v applies normal entry %v at index=%v,term=%v",
		g, e.Type, e.Index, e.Term)

	if len(e.Data) == 0 {
		glog.V(3).Infof("%v raft entry %v has no data", g, e.Index)
		return nil
	}

	id, req, err := g.node.decReq(e.Data)
	if err != nil {
		glog.Fatalf("%v cannot decode request: %v", g, err)
	}
	res := Response{ID: id}
	if req.Data != nil {
		res.Data, res.Err = g.stateMachine.Apply(req.Data)
	}
	g.node.line.call(res)
	return nil
}

func (n *MultiNode) decReq(data []byte) (id RequestID, req Request, err error) {

	dec := gob.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&id); err != nil {
		return id, req, err
	}

	var ok bool
	if id.Node == n.id {
		req, ok = n.line.get(id)
	}
	if !ok {
		err = dec.Decode(&req)
	}
	return id, req, nil
}

func (n *MultiNode) encReq(id RequestID, req Request) ([]byte, error) {
	var w bytes.Buffer
	enc := gob.NewEncoder(&w)
	if err := enc.Encode(id); err != nil {
		return nil, err
	}
	if err := enc.Encode(req); err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

func containsNode(nodes []uint64, node uint64) bool {
	for _, n := range nodes {
		if node == n {
			return true
		}
	}
	return false
}

func (g *group) validConfChange(cc raftpb.ConfChange) error {
	if cc.NodeID == etcdraft.None {
		return errors.New("node id is nil")
	}

	switch cc.Type {
	case raftpb.ConfChangeAddNode:
		if cc.NodeID != g.node.id && containsNode(g.confState.Nodes, cc.NodeID) {
			return fmt.Errorf("%v is duplicate", cc.NodeID)
		}

	case raftpb.ConfChangeRemoveNode:
		if !containsNode(g.confState.Nodes, cc.NodeID) {
			return fmt.Errorf("no such node %v", cc.NodeID)
		}

	default:
		glog.Fatalf("invalid ConfChange type %v", cc.Type)
	}
	return nil
}

func (g *group) applyConfChange(e raftpb.Entry) error {
	var cc raftpb.ConfChange
	pbutil.MustUnmarshal(&cc, e.Data)
	glog.V(2).Infof("%v applies conf change %v: %#v", g, e.Index, cc)

	if len(cc.Context) == 0 {
		g.stateMachine.ApplyConfChange(cc, GroupNode{})
		return nil
	}

	if id, req, err := g.node.decReq(cc.Context); err == nil {
		if gn, ok := req.Data.(GroupNode); ok {
			res := Response{ID: id}
			res.Err = g.stateMachine.ApplyConfChange(cc, gn)
			g.node.line.call(res)
			return nil
		}
	}

	var gn GroupNode
	if err := bhgob.Decode(&gn, cc.Context); err != nil {
		glog.Fatalf("%v cannot decode config change: %v", g, err)
	}

	if gn.Node != cc.NodeID {
		glog.Fatalf("invalid config change: %v != %v", gn.Node, cc.NodeID)
	}
	g.stateMachine.ApplyConfChange(cc, gn)
	return nil
}

func (g *group) snapshot() {
	d, err := g.stateMachine.Save()
	if err != nil {
		glog.Fatalf("error in seralizing the state machine: %v", err)
	}
	g.snapped = g.applied

	go func(snapi uint64) {
		snap, err := g.raftStorage.CreateSnapshot(snapi, &g.confState, d)
		if err != nil {
			// the snapshot was done asynchronously with the progress of raft.
			// raft might have already got a newer snapshot.
			if err == etcdraft.ErrSnapOutOfDate {
				return
			}
			glog.Fatalf("unexpected create snapshot error %v", err)
		}

		if err := g.diskStorage.SaveSnap(snap); err != nil {
			glog.Fatalf("save snapshot error: %v", err)
		}
		glog.Infof("%v saved snapshot at index %d", g, snap.Metadata.Index)

		// keep some in memory log entries for slow followers.
		compacti := uint64(1)
		if snapi > numberOfCatchUpEntries {
			compacti = snapi - numberOfCatchUpEntries
		}
		if err = g.raftStorage.Compact(compacti); err != nil {
			// the compaction was done asynchronously with the progress of raft.
			// raft log might already been compact.
			if err == etcdraft.ErrCompacted {
				return
			}
			glog.Fatalf("unexpected compaction error %v", err)
		}
		glog.Infof("%v compacted raft log at %d", g, compacti)
	}(g.snapped)
}

type groupRequestType int

const (
	groupRequestCreate groupRequestType = iota + 1
	groupRequestRemove
	groupRequestStatus
)

type groupRequest struct {
	reqType groupRequestType
	group   *group
	config  *etcdraft.Config
	peers   []etcdraft.Peer
	ch      chan groupResponse
}

type groupResponse struct {
	group uint64
	err   error
}

type multiMessage struct {
	group uint64
	msg   raftpb.Message
}

// MultiNode represents a node that participates in multiple
// raft consensus groups.
type MultiNode struct {
	id   uint64
	name string
	node etcdraft.MultiNode
	line line
	gen  *gen.SeqIDGen

	groups   map[uint64]*group
	groupc   chan groupRequest
	recvc    chan batchTimeout
	propc    chan multiMessage
	applyc   chan map[uint64]etcdraft.Ready
	advancec chan map[uint64]etcdraft.Ready

	send SendFunc

	pmu           sync.Mutex
	pendingElects map[uint64][]chan struct{}

	ticker <-chan time.Time
	stop   chan struct{}
	done   chan struct{}
}

// Config represents the configuration of a MultiNode.
type Config struct {
	ID     uint64           // Node ID.
	Name   string           // Node name.
	Send   SendFunc         // Network send function.
	Ticker <-chan time.Time // Ticker of the node.
}

// StartMultiNode starts a MultiNode with the given id and name. Send function
// is used send all messags of this node. The ticker is used for all groups.
// You can fine tune the hearbeat and election timeouts in the group configs.
func StartMultiNode(cfg Config) (node *MultiNode) {
	mn := etcdraft.StartMultiNode(cfg.ID)
	node = &MultiNode{
		id:            cfg.ID,
		name:          cfg.Name,
		node:          mn,
		gen:           gen.NewSeqIDGen(1),
		groups:        make(map[uint64]*group),
		groupc:        make(chan groupRequest),
		recvc:         make(chan batchTimeout, 16),
		propc:         make(chan multiMessage),
		applyc:        make(chan map[uint64]etcdraft.Ready),
		advancec:      make(chan map[uint64]etcdraft.Ready),
		send:          cfg.Send,
		pendingElects: make(map[uint64][]chan struct{}),
		ticker:        cfg.Ticker,
		stop:          make(chan struct{}),
		done:          make(chan struct{}),
	}
	node.line.init()
	go node.start()
	go node.startApplier()
	return
}

func (n *MultiNode) startApplier() {
	for {
		select {
		case rd := <-n.applyc:
			n.handleReadies(rd)
		case <-n.done:
			return
		}
	}
}

func (n *MultiNode) start() {
	glog.V(2).Infof("%v started", n)

	defer func() {
		n.node.Stop()
		close(n.done)
	}()

	readyc := n.node.Ready()
	groupc := n.groupc
	for {
		select {
		case <-n.ticker:
			n.node.Tick()

		case bt := <-n.recvc:
			ch := time.After(1 * time.Millisecond)
			n.handleBatch(bt)
		loopr:
			for {
				select {
				case bt := <-n.recvc:
					n.handleBatch(bt)
				case <-ch:
					break loopr
				default:
					break loopr
				}
			}

		case mm := <-n.propc:
			ch := time.After(1 * time.Millisecond)
			n.node.Step(context.TODO(), mm.group, mm.msg)
		loopp:
			for {
				select {
				case mm := <-n.propc:
					n.node.Step(context.TODO(), mm.group, mm.msg)
				case <-ch:
					break loopp
				default:
					break loopp
				}
			}

		case req := <-groupc:
			n.handleGroupRequest(req)

		case rd := <-readyc:
			groupc = nil
			readyc = nil
			n.applyc <- rd

		case rd := <-n.advancec:
			readyc = n.node.Ready()
			groupc = n.groupc
			n.node.Advance(rd)

		case <-n.stop:
			return
		}
	}
}

func (n *MultiNode) handleBatch(bt batchTimeout) {
	ctx, cnl := context.WithTimeout(context.Background(), bt.timeout)
	for g, msgs := range bt.batch.Messages {
		if _, ok := n.groups[g]; !ok {
			glog.Errorf("group %v is not created on %v", g, n)
			continue
		}
		for _, m := range msgs {
			if err := n.node.Step(ctx, g, m); err != nil {
				glog.Errorf("%v cannot step group %v: %v", n, g, err)
				if err == context.DeadlineExceeded || err == context.Canceled {
					return
				}
			}
		}
	}
	cnl()
}

type nodeBatch map[uint64]*Batch

func (nb nodeBatch) batch(node uint64) *Batch {
	b, ok := nb[node]
	if !ok {
		b = &Batch{
			Messages: make(map[uint64][]raftpb.Message),
		}
		nb[node] = b
	}
	return b
}

// isBefore returns whether lhs is before rhs.
func isBefore(lhs, rhs raftpb.Message) bool {
	return lhs.Type != rhs.Type || lhs.Term < rhs.Term || lhs.Index < rhs.Index
}

func shouldSave(rd etcdraft.Ready) bool {
	return rd.SoftState != nil || !etcdraft.IsEmptyHardState(rd.HardState) ||
		!etcdraft.IsEmptySnap(rd.Snapshot) || len(rd.Entries) > 0 ||
		len(rd.CommittedEntries) > 0
}

func (n *MultiNode) handleReadies(readies map[uint64]etcdraft.Ready) {
	glog.V(3).Infof("%v handles a ready", n)

	beatBatch := make(nodeBatch)
	normBatch := make(nodeBatch)
	snapBatch := make(nodeBatch)

	saved := make(chan struct{}, len(readies))
	for gid, rd := range readies {
		if !shouldSave(rd) {
			saved <- struct{}{}
			continue
		}

		g, ok := n.groups[gid]
		if !ok {
			glog.Fatalf("cannot find group %v", g)
		}
		g.savec <- readySaved{
			ready: rd,
			saved: saved,
		}
	}

	for gid, rd := range readies {
		for _, m := range rd.Messages {
			if m.Type == raftpb.MsgHeartbeat || m.Type == raftpb.MsgHeartbeatResp {
				batch := beatBatch.batch(m.To)
				// Only one heartbeat/response message should suffice.
				batch.Messages[gid] = []raftpb.Message{m}
				continue
			}

			var batch *Batch
			if !etcdraft.IsEmptySnap(m.Snapshot) {
				batch = snapBatch.batch(m.To)
			} else {
				batch = normBatch.batch(m.To)
			}
			batch.Messages[gid] = append(batch.Messages[gid], m)
		}
	}

	for i := 0; i < len(readies); i++ {
		select {
		case <-saved:
		case <-n.done:
			return
		}
	}

	glog.V(3).Infof("%v saved the readies for all groups", n)

	for nid, batch := range beatBatch {
		glog.V(3).Infof("%v sends high priority batch to %v", n, nid)
		batch.From = n.id
		batch.To = nid
		batch.Priority = High
		n.send(batch, n.node)
	}

	for nid, batch := range normBatch {
		glog.V(3).Infof("%v sends normal priority batch to %v", n, nid)
		batch.From = n.id
		batch.To = nid
		batch.Priority = Normal
		n.send(batch, n.node)
	}

	for nid, batch := range snapBatch {
		glog.V(3).Infof("%v sends low priority batch to %v", n, nid)
		batch.From = n.id
		batch.To = nid
		batch.Priority = Low
		n.send(batch, n.node)
	}

	select {
	case n.advancec <- readies:
	case <-n.done:
	}

}

// GroupConfig represents the configuration of a raft group.
type GroupConfig struct {
	ID             uint64          // Group ID.
	Name           string          // Group name.
	StateMachine   StateMachine    // Group state machine.
	Peers          []etcdraft.Peer // Peers of this group.
	DataDir        string          // Where to save raft state.
	SnapCount      uint64          // How many entries to include in a snapshot.
	SyncTime       time.Duration   // The frequency of fsyncs.
	ElectionTicks  int             // Number of ticks to fire an election.
	HeartbeatTicks int             // Number of ticks to fire heartbeats.
	MaxInFlights   int             // Maximum number of inflight messages.
	MaxMsgSize     uint64          // Maximum number of entries in a message.
}

func (n *MultiNode) CreateGroup(ctx context.Context, cfg GroupConfig) error {
	glog.V(2).Infof("creating a new group %v (%v) on node %v (%v) with peers %v",
		cfg.ID, cfg.Name, n.id, n.name, cfg.Peers)

	rs, ds, _, lei, _, err := OpenStorage(cfg.ID, cfg.DataDir, cfg.StateMachine)
	if err != nil {
		glog.Fatalf("cannot open storage: %v", err)
	}

	snap, err := rs.Snapshot()
	if err != nil {
		glog.Errorf("error in storage snapshot: %v", err)
		return err
	}

	n.gen.StartFrom((lei + cfg.SnapCount) << 8) // To avoid conflicts.

	c := &etcdraft.Config{
		ID:              cfg.ID,
		ElectionTick:    cfg.ElectionTicks,
		HeartbeatTick:   cfg.HeartbeatTicks,
		Storage:         rs,
		MaxSizePerMsg:   cfg.MaxMsgSize,
		MaxInflightMsgs: cfg.MaxInFlights,
		// TODO(soheil): Figure this one out:
		//               Applied: lsi,
	}
	g := &group{
		node:         n,
		id:           cfg.ID,
		name:         cfg.Name,
		stateMachine: cfg.StateMachine,
		raftStorage:  rs,
		diskStorage:  ds,
		applyc:       make(chan etcdraft.Ready, cfg.SnapCount),
		savec:        make(chan readySaved, 1),
		syncTime:     cfg.SyncTime,
		snapCount:    cfg.SnapCount,
		snapped:      snap.Metadata.Index,
		applied:      snap.Metadata.Index,
		confState:    snap.Metadata.ConfState,
		stopc:        make(chan struct{}),
		applierDone:  make(chan struct{}),
		saverDone:    make(chan struct{}),
	}
	ch := make(chan groupResponse, 1)
	n.groupc <- groupRequest{
		reqType: groupRequestCreate,
		group:   g,
		config:  c,
		peers:   cfg.Peers,
		ch:      ch,
	}
	select {
	case res := <-ch:
		return res.err
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return ErrStopped
	}
}

func (n *MultiNode) handleGroupRequest(req groupRequest) {
	_, ok := n.groups[req.group.id]
	res := groupResponse{
		group: req.group.id,
	}

	switch req.reqType {
	case groupRequestCreate:
		if ok {
			res.err = ErrGroupExists
			break
		}

		n.groups[req.group.id] = req.group
		err := n.node.CreateGroup(req.group.id, req.config, req.peers)
		if err == nil {
			go req.group.startSaver()
			go req.group.startApplier()
		} else {
			delete(n.groups, req.group.id)
		}

	case groupRequestRemove:
		if !ok {
			res.err = ErrNoSuchGroup
			break
		}

		g, ok := n.groups[req.group.id]
		if !ok {
			res.err = ErrNoSuchGroup
			break
		}

		g.stop()
		delete(n.groups, req.group.id)

	case groupRequestStatus:
		// TODO(soheil): add softstate to the response.
		if _, ok := n.groups[req.group.id]; !ok {
			res.err = ErrNoSuchGroup
		}

	default:
		glog.Fatalf("invalid group request: %v", req.reqType)
	}

	req.ch <- res
}

func (n *MultiNode) genID() RequestID {
	return RequestID{
		Node: n.id,
		Seq:  n.gen.GenID(),
	}
}

func (n *MultiNode) waitElection(ctx context.Context, group uint64) {
	ch := make(chan struct{})
	n.pmu.Lock()
	n.pendingElects[group] = append(n.pendingElects[group], ch)
	n.pmu.Unlock()
	select {
	case <-ch:
	case <-ctx.Done():
	case <-n.done:
	}
}

func (n *MultiNode) notifyElection(group uint64) {
	n.pmu.Lock()
	chs := n.pendingElects[group]
	if len(chs) != 0 {
		for _, ch := range chs {
			close(ch)
		}
	}
	delete(n.pendingElects, group)
	n.pmu.Unlock()
}

// Propose proposes the request and returns the response. This method blocks and
// returns either when the ctx is cancelled or the raft node returns a response.
func (n *MultiNode) Propose(ctx context.Context, group uint64,
	req interface{}) (res interface{}, err error) {

	id := n.genID()
	r := Request{
		Data: req,
	}

	d, err := n.encReq(id, r)
	if err != nil {
		return nil, err
	}

	glog.V(2).Infof("%v waits on raft request %v", n, id)
	ch := n.line.wait(id, r)
	mm := multiMessage{group,
		raftpb.Message{
			Type:    raftpb.MsgProp,
			Entries: []raftpb.Entry{{Data: d}},
		}}
	select {
	case n.propc <- mm:
	case <-ctx.Done():
		n.line.cancel(id)
		return nil, ctx.Err()
	case <-n.done:
		return nil, ErrStopped
	}

	select {
	case res := <-ch:
		glog.V(2).Infof("%v wakes up for raft request %v", n, id)
		return res.Data, res.Err
	case <-ctx.Done():
		n.line.cancel(id)
		return nil, ctx.Err()
	case <-n.done:
		return nil, ErrStopped
	}
}

// ProposeRetry proposes the request to the given group. It retires maxRetries
// times using the given timeout. If maxRetries is -1 it will keep proposing the
// request until it retrieves a response.
func (n *MultiNode) ProposeRetry(group uint64, req interface{},
	timeout time.Duration, maxRetries int) (res interface{}, err error) {

	for {
		ctx, ccl := context.WithTimeout(context.Background(), timeout)
		defer ccl()
		if status := n.Status(group); status == nil || status.SoftState.Lead == 0 {
			// wait with the hope that the group will be created.
			n.waitElection(ctx, group)
		} else {
			res, err = n.Propose(ctx, group, req)
			if err != context.DeadlineExceeded {
				return
			}
		}

		if maxRetries < 0 {
			continue
		}

		maxRetries--
		if maxRetries == 0 {
			break
		}
	}

	return nil, context.DeadlineExceeded
}

func (n *MultiNode) AddNodeToGroup(ctx context.Context, node, group uint64,
	data interface{}) error {

	cc := raftpb.ConfChange{
		ID:     0,
		Type:   raftpb.ConfChangeAddNode,
		NodeID: node,
	}
	gn := GroupNode{
		Group: group,
		Node:  node,
		Data:  data,
	}
	return n.processConfChange(ctx, group, cc, gn)
}

func (n *MultiNode) RemoveNodeFromGroup(ctx context.Context, node, group uint64,
	data interface{}) error {

	cc := raftpb.ConfChange{
		ID:     0,
		Type:   raftpb.ConfChangeRemoveNode,
		NodeID: node,
	}
	gn := GroupNode{
		Group: group,
		Node:  node,
		Data:  data,
	}
	return n.processConfChange(ctx, group, cc, gn)
}

func (n *MultiNode) processConfChange(ctx context.Context, group uint64,
	cc raftpb.ConfChange, gn GroupNode) error {

	if group == 0 || gn.Node == 0 || gn.Group != group {
		glog.Fatalf("invalid group node: %v", gn)
	}

	id := n.genID()
	req := Request{Data: gn}

	var err error
	cc.Context, err = n.encReq(id, req)
	if err != nil {
		return err
	}

	ch := n.line.wait(id, req)

	d, err := cc.Marshal()
	if err != nil {
		return err
	}
	select {
	case n.propc <- multiMessage{
		group: group,
		msg: raftpb.Message{
			Type:    raftpb.MsgProp,
			Entries: []raftpb.Entry{{Type: raftpb.EntryConfChange, Data: d}},
		},
	}:
	case <-ctx.Done():
		n.line.cancel(id)
		return ctx.Err()
	case <-n.done:
		return ErrStopped
	}

	select {
	case res := <-ch:
		return res.Err
	case <-ctx.Done():
		n.line.cancel(id)
		return ctx.Err()
	case <-n.done:
		return ErrStopped
	}
}

func (n *MultiNode) String() string {
	return fmt.Sprintf("node %v (%v)", n.id, n.name)
}

func (n *MultiNode) Stop() {
	select {
	case n.stop <- struct{}{}:
	case <-n.done:
	}
	<-n.done
	for _, g := range n.groups {
		g.stop()
	}
}

func (n *MultiNode) Exists(ctx context.Context, gid uint64) (ok bool) {
	ch := make(chan groupResponse, 1)
	n.groupc <- groupRequest{
		reqType: groupRequestStatus,
		group:   &group{id: gid},
		ch:      ch,
	}
	select {
	case res := <-ch:
		return res.err == nil
	case <-ctx.Done():
		return false
	case <-n.done:
		return false
	}
}

// Campaign instructs the node to campign for the given group.
func (n *MultiNode) Campaign(ctx context.Context, group uint64) error {
	if !n.Exists(ctx, group) {
		return fmt.Errorf("raft node: group %v is not created on %v", group, n)
	}
	return n.node.Campaign(ctx, group)
}

type batchTimeout struct {
	timeout time.Duration
	batch   Batch
}

// StepBatch steps all messages in the batch. Context is used for enqueuing the
// batch and timeout is used as the maximum wait time when stepping messages
// in the batch.
func (n *MultiNode) StepBatch(ctx context.Context, batch Batch,
	timeout time.Duration) error {

	bt := batchTimeout{
		batch:   batch,
		timeout: timeout,
	}
	select {
	case n.recvc <- bt:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Status returns the latest status of the group. Returns nil if the group
// does not exists.
func (n *MultiNode) Status(group uint64) *etcdraft.Status {
	return n.node.Status(group)
}

func init() {
	gob.Register(Batch{})
	gob.Register(GroupNode{})
	gob.Register(RequestID{})
	gob.Register(Request{})
	gob.Register(Response{})
}
