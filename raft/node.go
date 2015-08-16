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

type SendFunc func(group uint64, m []raftpb.Message, r Reporter)

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

type groupState struct {
	id           uint64
	name         string
	stateMachine StateMachine
	raftStorage  *etcdraft.MemoryStorage
	diskStorage  DiskStorage
	snapCount    uint64

	leader    uint64
	snapi     uint64
	appliedi  uint64
	confState raftpb.ConfState
}

type groupRequestType int

const (
	groupRequestCreate groupRequestType = iota + 1
	groupRequestRemove
)

type groupRequest struct {
	reqType groupRequestType
	group   *groupState
	config  *etcdraft.Config
	peers   []etcdraft.Peer
	ch      chan error
}

type Node struct {
	sync.RWMutex

	id   uint64
	name string
	node etcdraft.MultiNode
	line line
	gen  *gen.SeqIDGen

	groups map[uint64]*groupState
	groupc chan groupRequest

	advancec chan map[uint64]etcdraft.Ready

	send SendFunc

	ticker <-chan time.Time
	stop   chan struct{}
	done   chan struct{}
}

func StartMultiNode(id uint64, name string, send SendFunc,
	ticker <-chan time.Time) (node *Node) {

	mn := etcdraft.StartMultiNode(id)
	node = &Node{
		id:       id,
		name:     name,
		node:     mn,
		gen:      gen.NewSeqIDGen(1),
		groups:   make(map[uint64]*groupState),
		groupc:   make(chan groupRequest),
		advancec: make(chan map[uint64]etcdraft.Ready),
		send:     send,
		ticker:   ticker,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	node.line.init()
	go node.start()
	return
}

func (n *Node) start() {
	glog.V(2).Infof("%v started", n)

	defer func() {
		n.node.Stop()
		for _, gs := range n.groups {
			if err := gs.diskStorage.Close(); err != nil {
				glog.Fatalf("error in closing storage of group %v: %v", gs.id, err)
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
	for g, rd := range readies {
		gs, ok := n.groups[g]
		if !ok {
			glog.Fatalf("cannot find group %v", g)
		}

		if rd.SoftState != nil {
			newLead := rd.SoftState.Lead
			if gs.leader != newLead {
				gs.stateMachine.ProcessStatusChange(LeaderChanged{
					Old:  gs.leader,
					New:  newLead,
					Term: rd.HardState.Term,
				})
				gs.leader = newLead
			}
		}

		// Apply snapshot to storage if it is more updated than current snapi.
		empty := etcdraft.IsEmptySnap(rd.Snapshot)
		if !empty && rd.Snapshot.Metadata.Index > gs.snapi {
			if err := gs.diskStorage.SaveSnap(rd.Snapshot); err != nil {
				glog.Fatalf("err in save snapshot: %v", err)
			}
			gs.raftStorage.ApplySnapshot(rd.Snapshot)
			gs.snapi = rd.Snapshot.Metadata.Index
			glog.Infof("saved incoming snapshot at index %d", gs.snapi)
		}

		if err := gs.diskStorage.Save(rd.HardState, rd.Entries); err != nil {
			glog.Fatalf("err in raft storage save: %v", err)
		}
		gs.raftStorage.Append(rd.Entries)

		n.send(g, rd.Messages, n.node)

		// Recover from snapshot if it is more recent than the currently
		// applied.
		if !empty && rd.Snapshot.Metadata.Index > gs.appliedi {
			if err := gs.stateMachine.Restore(rd.Snapshot.Data); err != nil {
				glog.Fatalf("error in recovering the state machine: %v", err)
			}
			// FIXME(soheil): update the nodes and notify the application?
			gs.appliedi = rd.Snapshot.Metadata.Index
			glog.Infof("recovered from incoming snapshot at index %d", gs.snapi)
		}

		if len(rd.CommittedEntries) != 0 {
			glog.V(2).Infof("%v receives raft update", n)
			firsti := rd.CommittedEntries[0].Index
			if firsti > gs.appliedi+1 {
				glog.Fatalf(
					"1st index of committed entry[%d] should <= appliedi[%d] + 1",
					firsti, gs.appliedi)
			}
			var ents []raftpb.Entry
			if gs.appliedi+1-firsti < uint64(len(rd.CommittedEntries)) {
				ents = rd.CommittedEntries[gs.appliedi+1-firsti:]
			}
			if len(ents) > 0 {
				stop := false
				if gs.appliedi, stop = n.apply(gs.id, ents, &gs.confState); stop {
					go n.Stop()
					return
				}
			}
		}

		if gs.appliedi-gs.snapi > gs.snapCount {
			glog.Infof("start to snapshot (applied: %d, lastsnap: %d)", gs.appliedi,
				gs.snapi)
			gs.snapshot()
		}
	}
	select {
	case n.advancec <- readies:
	case <-n.done:
	}
}

func (n *Node) CreateGroup(group uint64, name string, peers []etcdraft.Peer,
	datadir string, stateMachine StateMachine, snapCount uint64,
	election, heartbeat, maxInFlights int, maxMsgSize uint64) error {

	glog.V(2).Infof("creating a new group %v (%v) on node %v (%v) with peers %v",
		group, name, n.id, n.name, peers)

	rs, ds, _, lei, _, err := OpenStorage(group, datadir, stateMachine)
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
		ID:              group,
		ElectionTick:    election,
		HeartbeatTick:   heartbeat,
		Storage:         rs,
		MaxSizePerMsg:   maxMsgSize,
		MaxInflightMsgs: maxInFlights,
		// TODO(soheil): Figure this one out:
		//               Applied: lsi,
	}
	g := &groupState{
		id:           group,
		name:         name,
		stateMachine: stateMachine,
		raftStorage:  rs,
		diskStorage:  ds,
		snapCount:    snapCount,
		snapi:        snap.Metadata.Index,
		appliedi:     snap.Metadata.Index,
		confState:    snap.Metadata.ConfState,
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
		if err != nil {
			delete(n.groups, req.group.id)
		}
		req.ch <- err

	case groupRequestRemove:
		if !ok {
			req.ch <- ErrNoSuchGroup
			return
		}

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

		if n.Status(group) == nil {
			// wait with the hope that the group will be created.
			time.Sleep(timeout)
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

func (n *Node) applyEntry(group uint64, e raftpb.Entry) {
	glog.V(3).Infof("%v applies normal entry %v: %#v", n, e.Index, e)

	if len(e.Data) == 0 {
		glog.V(3).Infof("raft entry %v has no data", e.Index)
		return
	}

	var req Request
	if err := req.Decode(e.Data); err != nil {
		glog.Fatalf("raftserver: cannot decode entry data %v", err)
	}

	res := Response{
		ID: req.ID,
	}
	if req.Data != nil {
		res.Data, res.Err = n.groups[group].stateMachine.Apply(req.Data)
	}
	n.line.call(res)
}

func containsNode(nodes []uint64, node uint64) bool {
	for _, n := range nodes {
		if node == n {
			return true
		}
	}
	return false
}

func (n *Node) validConfChange(cc raftpb.ConfChange,
	confs *raftpb.ConfState) error {

	if cc.NodeID == etcdraft.None {
		return errors.New("node id is nil")
	}

	switch cc.Type {
	case raftpb.ConfChangeAddNode:
		if cc.NodeID != n.id && containsNode(confs.Nodes, cc.NodeID) {
			return fmt.Errorf("%v is duplicate", cc.NodeID)
		}

	case raftpb.ConfChangeRemoveNode:
		if !containsNode(confs.Nodes, cc.NodeID) {
			return fmt.Errorf("no such node %v", cc.NodeID)
		}

	default:
		glog.Fatalf("invalid ConfChange type %v", cc.Type)
	}
	return nil
}

func (n *Node) applyConfChange(group uint64, e raftpb.Entry,
	confs *raftpb.ConfState) error {

	var cc raftpb.ConfChange
	pbutil.MustUnmarshal(&cc, e.Data)
	glog.V(2).Infof("%v/%v applies conf change %v: %#v", group, n, e.Index, cc)

	if err := n.validConfChange(cc, confs); err != nil {
		glog.Errorf("%v received an invalid conf change for node %v: %v",
			n, cc.NodeID, err)
		cc.NodeID = etcdraft.None
		n.node.ApplyConfChange(group, cc)
		return err
	}

	g := n.groups[group]
	*confs = *n.node.ApplyConfChange(group, cc)
	if len(cc.Context) == 0 {
		g.stateMachine.ApplyConfChange(cc, GroupNode{})
		return nil
	}

	var req Request
	if err := req.Decode(cc.Context); err != nil {
		// It should be either a node info or a request.
		glog.Fatalf("raftserver: cannot decode context (%v)", err)
	}
	if gn, ok := req.Data.(GroupNode); ok {
		res := Response{
			ID: req.ID,
		}
		res.Err = g.stateMachine.ApplyConfChange(cc, gn)
		n.line.call(res)
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

func (n *Node) apply(group uint64, es []raftpb.Entry, confs *raftpb.ConfState) (
	appliedi uint64, shouldStop bool) {

	for _, e := range es {
		switch e.Type {
		case raftpb.EntryNormal:
			n.applyEntry(group, e)

		case raftpb.EntryConfChange:
			n.applyConfChange(group, e, confs)

		default:
			glog.Fatalf("unexpected entry type")
		}
		appliedi = e.Index
	}
	return
}

func (gs *groupState) snapshot() {
	d, err := gs.stateMachine.Save()
	if err != nil {
		glog.Fatalf("error in seralizing the state machine: %v", err)
	}

	go func() {
		snap, err := gs.raftStorage.CreateSnapshot(gs.snapi, &gs.confState, d)
		if err != nil {
			// the snapshot was done asynchronously with the progress of raft.
			// raft might have already got a newer snapshot.
			if err == etcdraft.ErrSnapOutOfDate {
				return
			}
			glog.Fatalf("unexpected create snapshot error %v", err)
		}

		if err := gs.diskStorage.SaveSnap(snap); err != nil {
			glog.Fatalf("save snapshot error: %v", err)
		}
		glog.Infof("saved snapshot at index %d", snap.Metadata.Index)

		// keep some in memory log entries for slow followers.
		compacti := uint64(1)
		if gs.snapi > numberOfCatchUpEntries {
			compacti = gs.snapi - numberOfCatchUpEntries
		}
		if err = gs.raftStorage.Compact(compacti); err != nil {
			// the compaction was done asynchronously with the progress of raft.
			// raft log might already been compact.
			if err == etcdraft.ErrCompacted {
				return
			}
			glog.Fatalf("unexpected compaction error %v", err)
		}
		glog.Infof("compacted raft log at %d", compacti)
	}()

	gs.snapi = gs.appliedi
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
