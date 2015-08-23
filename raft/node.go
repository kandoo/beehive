package raft

import (
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

// GroupMessages wraps the messages to be sent to a group.
type GroupMessages struct {
	Group    uint64
	Messages []raftpb.Message
}

// SendFunc sent a batch of messages to a group.
type SendFunc func(node uint64, gms []GroupMessages, r Reporter)

// GroupNode stores the ID, the group and an application-defined data for a
// a node in a raft group.
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

type group struct {
	node *Node

	id           uint64
	name         string
	stateMachine StateMachine
	raftStorage  *etcdraft.MemoryStorage
	diskStorage  DiskStorage
	snapCount    uint64

	leader    uint64
	confState raftpb.ConfState

	applyc  chan etcdraft.Ready
	applied uint64
	snapmu  sync.RWMutex
	snapped uint64

	stopc chan struct{}
	donec chan struct{}
}

func (g *group) String() string {
	return fmt.Sprintf("group %v (%v)", g.id, g.name)
}

func (g *group) start() {
	defer close(g.donec)
	for {
		select {
		case ents := <-g.applyc:
			if err := g.apply(ents); err != nil {
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
	case <-g.donec:
	}
	<-g.donec
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

		if newLead != 0 {
			g.node.notifyElection(g.id)
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

	glog.V(3).Infof("%v receives raft update", g)
	firsti := es[0].Index
	if firsti > g.applied+1 {
		glog.Fatalf(
			"1st index of committed entry[%d] should <= applied[%d] + 1",
			firsti, g.applied)
	}

	if len(es) == 0 {
		return nil
	}

	for _, e := range es {
		if g.applied > e.Index {
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

	var req Request
	if err := req.Decode(e.Data); err != nil {
		glog.Fatalf("raftserver: cannot decode entry data %v", err)
	}

	res := Response{
		ID: req.ID,
	}

	if req.Data != nil {
		res.Data, res.Err = g.stateMachine.Apply(req.Data)
	}
	g.node.line.call(res)
	return nil
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

	if err := g.validConfChange(cc); err != nil {
		glog.Errorf("%v received an invalid conf change for node %v: %v",
			g, cc.NodeID, err)
		cc.NodeID = etcdraft.None
		g.node.node.ApplyConfChange(g.id, cc)
		return err
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

	if len(cc.Context) == 0 {
		g.stateMachine.ApplyConfChange(cc, GroupNode{})
		return nil
	}

	var req Request
	if err := req.Decode(cc.Context); err != nil {
		// It should be either a node info or a request.
		glog.Fatalf("cannot decode context (%v)", err)
	}
	if gn, ok := req.Data.(GroupNode); ok {
		res := Response{
			ID: req.ID,
		}
		res.Err = g.stateMachine.ApplyConfChange(cc, gn)
		g.node.line.call(res)
		return nil
	}

	var gn GroupNode
	if err := bhgob.Decode(&gn, cc.Context); err == nil {
		if gn.Node != cc.NodeID {
			glog.Fatalf("invalid config change: %v != %v", gn.Node, cc.NodeID)
		}
		g.stateMachine.ApplyConfChange(cc, gn)
		return nil
	}

	glog.Fatalf("cannot decode config change")
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
)

type groupRequest struct {
	reqType groupRequestType
	group   *group
	config  *etcdraft.Config
	peers   []etcdraft.Peer
	ch      chan error
}

type Node struct {
	id   uint64
	name string
	node etcdraft.MultiNode
	line line
	gen  *gen.SeqIDGen

	groups map[uint64]*group
	groupc chan groupRequest

	advancec chan map[uint64]etcdraft.Ready

	send SendFunc

	pmu           sync.Mutex
	pendingElects map[uint64][]chan struct{}

	ticker <-chan time.Time
	stop   chan struct{}
	done   chan struct{}
}

func StartMultiNode(id uint64, name string, send SendFunc,
	ticker <-chan time.Time) (node *Node) {

	mn := etcdraft.StartMultiNode(id)
	node = &Node{
		id:            id,
		name:          name,
		node:          mn,
		gen:           gen.NewSeqIDGen(1),
		groups:        make(map[uint64]*group),
		groupc:        make(chan groupRequest),
		advancec:      make(chan map[uint64]etcdraft.Ready),
		send:          send,
		pendingElects: make(map[uint64][]chan struct{}),
		ticker:        ticker,
		stop:          make(chan struct{}),
		done:          make(chan struct{}),
	}
	node.line.init()
	go node.start()
	return
}

func (n *Node) start() {
	glog.V(2).Infof("%v started", n)

	defer func() {
		n.node.Stop()
		for _, g := range n.groups {
			if err := g.diskStorage.Close(); err != nil {
				glog.Fatalf("error in closing storage of group %v: %v", g.id, err)
			}
		}
		close(n.done)
	}()

	readyc := n.node.Ready()
	groupc := n.groupc
	for {
		select {
		case <-n.ticker:
			n.node.Tick()

		case req := <-groupc:
			n.handleGroupRequest(req)

		case rd := <-readyc:
			groupc = nil
			readyc = nil
			go n.handleReadies(rd)

		case rd := <-n.advancec:
			readyc = n.node.Ready()
			groupc = n.groupc
			n.node.Advance(rd)

		case <-n.stop:
			return
		}
	}
}

func (n *Node) handleReadies(readies map[uint64]etcdraft.Ready) {
	normMsgs := make(map[uint64]map[uint64][]raftpb.Message)
	snapMsgs := make(map[uint64]map[uint64][]raftpb.Message)

	saved := make(chan struct{}, len(readies))
	for gid, rd := range readies {
		go func(gid uint64, rd etcdraft.Ready) {
			g, ok := n.groups[gid]
			if !ok {
				glog.Fatalf("cannot find group %v", g)
			}

			// Apply snapshot to storage if it is more updated than current snapped.
			if !etcdraft.IsEmptySnap(rd.Snapshot) {
				if err := g.diskStorage.SaveSnap(rd.Snapshot); err != nil {
					glog.Fatalf("err in save snapshot: %v", err)
				}
				g.raftStorage.ApplySnapshot(rd.Snapshot)
				glog.Infof("%v saved incoming snapshot at index %d", n,
					rd.Snapshot.Metadata.Index)
			}

			if err := g.diskStorage.Save(rd.HardState, rd.Entries); err != nil {
				glog.Fatalf("err in raft storage save: %v", err)
			}
			g.raftStorage.Append(rd.Entries)

			saved <- struct{}{}

			select {
			case g.applyc <- rd:
			case <-n.done:
			}
		}(gid, rd)

		for _, m := range rd.Messages {
			nMsgs := normMsgs
			if !etcdraft.IsEmptySnap(m.Snapshot) {
				nMsgs = snapMsgs
			}
			gMsgs, ok := nMsgs[m.To]
			if !ok {
				gMsgs = make(map[uint64][]raftpb.Message)
				nMsgs[m.To] = gMsgs
			}
			gMsgs[gid] = append(gMsgs[gid], m)
		}
	}

	for i := 0; i < len(readies); i++ {
		select {
		case <-saved:
		case <-n.done:
			return
		}
	}

	for nid, gMsgs := range normMsgs {
		gms := make([]GroupMessages, 0, len(gMsgs))
		for gid, msgs := range gMsgs {
			gms = append(gms, GroupMessages{Group: gid, Messages: msgs})
		}
		n.send(nid, gms, n.node)
	}

	for nid, gMsgs := range snapMsgs {
		gms := make([]GroupMessages, 0, len(gMsgs))
		for gid, msgs := range gMsgs {
			gms = append(gms, GroupMessages{Group: gid, Messages: msgs})
		}
		n.send(nid, gms, n.node)
	}

	select {
	case n.advancec <- readies:
	case <-n.done:
	}
}

func (n *Node) CreateGroup(id uint64, name string, peers []etcdraft.Peer,
	datadir string, stateMachine StateMachine, snapCount uint64,
	electionTicks, heartbeatTicks, maxInFlights int, maxMsgSize uint64) error {

	glog.V(2).Infof("creating a new group %v (%v) on node %v (%v) with peers %v",
		id, name, n.id, n.name, peers)

	rs, ds, _, lei, _, err := OpenStorage(id, datadir, stateMachine)
	if err != nil {
		glog.Fatalf("cannot open storage: %v", err)
	}

	snap, err := rs.Snapshot()
	if err != nil {
		glog.Errorf("error in storage snapshot: %v", err)
		return err
	}

	n.gen.StartFrom((lei + snapCount) << 8) // To avoid conflicts.

	c := &etcdraft.Config{
		ID:              id,
		ElectionTick:    electionTicks,
		HeartbeatTick:   heartbeatTicks,
		Storage:         rs,
		MaxSizePerMsg:   maxMsgSize,
		MaxInflightMsgs: maxInFlights,
		// TODO(soheil): Figure this one out:
		//               Applied: lsi,
	}
	g := &group{
		node:         n,
		id:           id,
		name:         name,
		stateMachine: stateMachine,
		raftStorage:  rs,
		diskStorage:  ds,
		applyc:       make(chan etcdraft.Ready, snapCount),
		snapCount:    snapCount,
		snapped:      snap.Metadata.Index,
		applied:      snap.Metadata.Index,
		confState:    snap.Metadata.ConfState,
		stopc:        make(chan struct{}),
		donec:        make(chan struct{}),
	}
	ch := make(chan error, 1)
	n.groupc <- groupRequest{
		reqType: groupRequestCreate,
		group:   g,
		config:  c,
		peers:   peers,
		ch:      ch,
	}
	return <-ch
}

func (n *Node) handleGroupRequest(req groupRequest) {
	_, ok := n.groups[req.group.id]
	switch req.reqType {
	case groupRequestCreate:
		if ok {
			req.ch <- ErrGroupExists
			return
		}

		n.groups[req.group.id] = req.group
		err := n.node.CreateGroup(req.group.id, req.config, req.peers)
		if err == nil {
			go req.group.start()
		} else {
			delete(n.groups, req.group.id)
		}
		req.ch <- err

	case groupRequestRemove:
		if !ok {
			req.ch <- ErrNoSuchGroup
			return
		}

		g, ok := n.groups[req.group.id]
		if !ok {
			return
		}

		g.stop()
		delete(n.groups, req.group.id)
	}

	req.ch <- nil
}

func (n *Node) genID() RequestID {
	return RequestID{
		Node: n.id,
		Seq:  n.gen.GenID(),
	}
}

func (n *Node) waitElection(ctx context.Context, group uint64) {
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

func (n *Node) notifyElection(group uint64) {
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
func (n *Node) Propose(ctx context.Context, group uint64, req interface{}) (
	res interface{}, err error) {

	r := Request{
		ID:   n.genID(),
		Data: req,
	}

	b, err := r.Encode()
	if err != nil {
		return nil, err
	}

	glog.V(2).Infof("%v waits on raft request %v: %#v", n, r.ID, req)
	ch := n.line.wait(r.ID)
	n.node.Propose(ctx, group, b)
	select {
	case res := <-ch:
		glog.V(2).Infof("%v wakes up for raft request %v", n, r.ID)
		return res.Data, res.Err
	case <-ctx.Done():
		n.line.cancel(r.ID)
		return nil, ctx.Err()
	case <-n.done:
		return nil, ErrStopped
	}
}

// ProposeRetry proposes the request to the given group. It retires maxRetries
// times using the given timeout. If maxRetries is -1 it will keep proposing the
// request until it retrieves a response.
func (n *Node) ProposeRetry(group uint64, req interface{},
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

func (n *Node) AddNodeToGroup(ctx context.Context, node, group uint64,
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

func (n *Node) RemoveNodeFromGroup(ctx context.Context, node, group uint64,
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

func (n *Node) processConfChange(ctx context.Context, group uint64,
	cc raftpb.ConfChange, gn GroupNode) error {

	r := Request{
		ID:   n.genID(),
		Data: gn,
	}
	var err error
	cc.Context, err = r.Encode()
	if err != nil {
		return err
	}

	if group == 0 || gn.Node == 0 || gn.Group != group {
		glog.Fatalf("invalid group node: %v", gn)
	}

	ch := n.line.wait(r.ID)
	n.node.ProposeConfChange(ctx, group, cc)
	select {
	case res := <-ch:
		return res.Err
	case <-ctx.Done():
		n.line.cancel(r.ID)
		return ctx.Err()
	case <-n.done:
		return ErrStopped
	}
}

func (n *Node) String() string {
	return fmt.Sprintf("node %v (%v)", n.id, n.name)
}

func (n *Node) Stop() {
	select {
	case n.stop <- struct{}{}:
	case <-n.done:
	}
	<-n.done
	for _, g := range n.groups {
		g.stop()
	}
}

// Campaign instructs the node to campign for the given group.
func (n *Node) Campaign(ctx context.Context, group uint64) error {
	if n.node.Status(group) == nil {
		return fmt.Errorf("raft node: group %v is not created on %v", group, n)
	}
	return n.node.Campaign(ctx, group)
}

// Step steps the raft node for the given group using msg.
func (n *Node) Step(ctx context.Context, group uint64, msg raftpb.Message) (
	err error) {

	if n.node.Status(group) == nil {
		return fmt.Errorf("raft node: group %v is not created on %v", group, n)
	}
	return n.node.Step(ctx, group, msg)
}

// Status returns the latest status of the group. Returns nil if the group
// does not exists.
func (n *Node) Status(group uint64) *etcdraft.Status {
	return n.node.Status(group)
}

func init() {
	gob.Register(GroupNode{})
	gob.Register(RequestID{})
	gob.Register(Request{})
	gob.Register(Response{})
}
