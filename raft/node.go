package raft

import (
	"encoding/gob"
	"errors"
	"fmt"
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
	ErrStopped = errors.New("node stopped")
	// ErrUnreachable is returned when SendFunc cannot reach the destination node.
	ErrUnreachable = errors.New("node unreachable")
)

type Reporter interface {
	// Report reports the given node is not reachable for the last send.
	ReportUnreachable(id uint64)
	// ReportSnapshot reports the stutus of the sent snapshot.
	ReportSnapshot(id uint64, status etcdraft.SnapshotStatus)
}

type SendFunc func(m []raftpb.Message, r Reporter)

// NodeInfo stores the ID and the address of a hive.
type NodeInfo struct {
	ID   uint64 `json:"id"`
	Addr string `json:"addr"`
}

// Peer returns a peer which stores the binary representation of the hive info
// in the the peer's context.
func (i NodeInfo) Peer() etcdraft.Peer {
	return etcdraft.Peer{
		ID:      i.ID,
		Context: i.MustEncode(),
	}
}

// MustEncode encodes the hive into bytes.
func (i NodeInfo) MustEncode() []byte {
	b, err := bhgob.Encode(i)
	if err != nil {
		glog.Fatalf("error in encoding peer: %v", err)
	}
	return b
}

type Node struct {
	name string
	id   uint64
	node etcdraft.Node
	line line
	gen  gen.IDGenerator

	stateMachine StateMachine
	raftStorage  *etcdraft.MemoryStorage
	diskStorage  DiskStorage
	snapCount    uint64

	send SendFunc

	ticker <-chan time.Time
	stop   chan struct{}
	done   chan struct{}
}

func init() {
	gob.Register(NodeInfo{})
	gob.Register(RequestID{})
	gob.Register(Request{})
	gob.Register(Response{})
}

func NewNode(name string, id uint64, peers []etcdraft.Peer, send SendFunc,
	datadir string, stateMachine StateMachine, snapCount uint64,
	ticker <-chan time.Time, election, heartbeat, maxInFlights int,
	maxMsgSize uint64) (node *Node) {

	glog.V(2).Infof("creating a new raft node %v (%v) with peers %v", id, name,
		peers)

	rs, ds, _, lei, exists, err := OpenStorage(id, datadir, stateMachine)
	if err != nil {
		glog.Fatalf("cannot open storage: %v", err)
	}

	c := &etcdraft.Config{
		ID:              id,
		ElectionTick:    election,
		HeartbeatTick:   heartbeat,
		Storage:         rs,
		MaxSizePerMsg:   maxMsgSize,
		MaxInflightMsgs: maxInFlights,
		// TODO(soheil): Figure this one out:
		//               Applied: lsi,
	}

	var n etcdraft.Node
	if !exists {
		n = etcdraft.StartNode(c, peers)
	} else {
		n = etcdraft.RestartNode(c)
	}

	node = &Node{
		name:         name,
		id:           id,
		node:         n,
		gen:          gen.NewSeqIDGen(lei + 2*snapCount), // avoid collisions.
		stateMachine: stateMachine,
		raftStorage:  rs,
		diskStorage:  ds,
		snapCount:    snapCount,
		send:         send,
		ticker:       ticker,
		done:         make(chan struct{}),
		stop:         make(chan struct{}),
	}
	node.line.init()
	go node.Start()
	return
}

func (n *Node) genID() RequestID {
	return RequestID{
		NodeID: n.id,
		Seq:    n.gen.GenID(),
	}
}

// Process processes the request and returns the response. It is blocking.
func (n *Node) Process(ctx context.Context, req interface{}) (interface{},
	error) {

	r := Request{
		ID:   n.genID(),
		Data: req,
	}

	b, err := r.Encode()
	if err != nil {
		return Response{}, err
	}

	glog.V(2).Infof("%v waits on raft request %v: %#v", n, r.ID, req)
	ch := n.line.wait(r.ID)
	n.node.Propose(ctx, b)
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

func (n *Node) AddNode(ctx context.Context, id uint64, addr string) error {
	cc := raftpb.ConfChange{
		ID:     0,
		Type:   raftpb.ConfChangeAddNode,
		NodeID: id,
	}
	return n.ProcessConfChange(ctx, cc, NodeInfo{ID: id, Addr: addr})
}

func (n *Node) RemoveNode(ctx context.Context, id uint64, addr string) error {
	cc := raftpb.ConfChange{
		ID:     0,
		Type:   raftpb.ConfChangeRemoveNode,
		NodeID: id,
	}
	return n.ProcessConfChange(ctx, cc, NodeInfo{ID: id, Addr: addr})
}

func (n *Node) ProcessConfChange(ctx context.Context, cc raftpb.ConfChange,
	info NodeInfo) error {

	r := Request{
		ID:   n.genID(),
		Data: info,
	}
	var err error
	cc.Context, err = r.Encode()
	if err != nil {
		return err
	}

	ch := n.line.wait(r.ID)
	n.node.ProposeConfChange(ctx, cc)
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

func (n *Node) applyEntry(e raftpb.Entry) {
	glog.V(3).Infof("%v applies normal entry %v: %#v", n, e.Index, e)

	if len(e.Data) == 0 {
		glog.V(3).Infof("raft entry %v has no data", e.Index)
		return
	}

	var req Request
	if err := req.Decode(e.Data); err != nil {
		glog.Fatalf("raftserver: cannot decode entry data %v", err)
	}

	if req.Data == nil {
		return
	}
	res := Response{
		ID: req.ID,
	}
	res.Data, res.Err = n.stateMachine.Apply(req.Data)
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
		if containsNode(confs.Nodes, cc.NodeID) {
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

func (n *Node) applyConfChange(e raftpb.Entry, confs *raftpb.ConfState) error {
	var cc raftpb.ConfChange
	pbutil.MustUnmarshal(&cc, e.Data)
	glog.V(2).Infof("%v applies conf change %v: %#v", n, e.Index, cc)

	if err := n.validConfChange(cc, confs); err != nil {
		glog.Errorf("%v received an invalid conf change for node %v: %v",
			n, cc.NodeID, err)
		cc.NodeID = etcdraft.None
		n.node.ApplyConfChange(cc)
		return err
	}

	*confs = *n.node.ApplyConfChange(cc)
	if len(cc.Context) == 0 {
		n.stateMachine.ApplyConfChange(cc, NodeInfo{})
		return nil
	}

	var info NodeInfo
	if err := bhgob.Decode(&info, cc.Context); err == nil {
		if info.ID != cc.NodeID {
			glog.Fatalf("invalid config change: %v != %v", info.ID, cc.NodeID)
		}
		n.stateMachine.ApplyConfChange(cc, info)
		return nil
	}

	var req Request
	if err := req.Decode(cc.Context); err != nil {
		// It should be either a node info or a request.
		glog.Fatalf("raftserver: cannot decode context (%v)", err)
	}
	res := Response{
		ID: req.ID,
	}
	res.Err = n.stateMachine.ApplyConfChange(cc, req.Data.(NodeInfo))
	n.line.call(res)
	return nil
}

func (n *Node) Start() {
	glog.V(2).Infof("%v started", n)

	snap, err := n.raftStorage.Snapshot()
	if err != nil {
		glog.Fatalf("error in storage snapshot: %v", err)
	}

	snapi := snap.Metadata.Index
	appliedi := snap.Metadata.Index
	confState := snap.Metadata.ConfState

	var oldLead uint64
	var shouldStop bool

	defer func() {
		n.node.Stop()
		if err := n.diskStorage.Close(); err != nil {
			glog.Fatalf("error in storage close: %v", err)
		}
		close(n.done)
	}()

	ready := n.node.Ready()
	adv := make(chan struct{})
	for {
		select {
		case <-n.ticker:
			n.node.Tick()

		case <-adv:
			ready = n.node.Ready()
			n.node.Advance()

		case rd := <-ready:
			ready = nil
			go func(rd etcdraft.Ready) {
				if rd.SoftState != nil {
					newLead := rd.SoftState.Lead
					if oldLead != newLead {
						n.stateMachine.ProcessStatusChange(LeaderChanged{
							Old:  oldLead,
							New:  newLead,
							Term: rd.HardState.Term,
						})
						oldLead = newLead
					}
				}

				empty := etcdraft.IsEmptySnap(rd.Snapshot)

				// Apply snapshot to storage if it is more updated than current snapi.
				if !empty && rd.Snapshot.Metadata.Index > snapi {
					if err := n.diskStorage.SaveSnap(rd.Snapshot); err != nil {
						glog.Fatalf("err in save snapshot: %v", err)
					}
					n.raftStorage.ApplySnapshot(rd.Snapshot)
					snapi = rd.Snapshot.Metadata.Index
					glog.Infof("saved incoming snapshot at index %d", snapi)
				}

				if err := n.diskStorage.Save(rd.HardState, rd.Entries); err != nil {
					glog.Fatalf("err in raft storage save: %v", err)
				}
				n.raftStorage.Append(rd.Entries)

				n.send(rd.Messages, n.node)

				// Recover from snapshot if it is more recent than the currently
				// applied.
				if !empty && rd.Snapshot.Metadata.Index > appliedi {
					if err := n.stateMachine.Restore(rd.Snapshot.Data); err != nil {
						glog.Fatalf("error in recovering the state machine: %v", err)
					}
					// FIXME(soheil): update the nodes and notify the application?
					appliedi = rd.Snapshot.Metadata.Index
					glog.Infof("recovered from incoming snapshot at index %d", snapi)
				}

				if len(rd.CommittedEntries) != 0 {
					glog.V(2).Infof("%v receives raft update", n)
					firsti := rd.CommittedEntries[0].Index
					if firsti > appliedi+1 {
						glog.Fatalf(
							"1st index of committed entry[%d] should <= appliedi[%d] + 1",
							firsti, appliedi)
					}
					var ents []raftpb.Entry
					if appliedi+1-firsti < uint64(len(rd.CommittedEntries)) {
						ents = rd.CommittedEntries[appliedi+1-firsti:]
					}
					if len(ents) > 0 {
						if appliedi, shouldStop = n.apply(ents, &confState); shouldStop {
							n.Stop()
							return
						}
					}
				}

				if appliedi-snapi > n.snapCount {
					glog.Infof("start to snapshot (applied: %d, lastsnap: %d)", appliedi,
						snapi)
					n.snapshot(appliedi, confState)
					snapi = appliedi
				}

				select {
				case adv <- struct{}{}:
				case <-n.done:
				}
			}(rd)

		case <-n.stop:
			return
		}
	}
}

func (n *Node) apply(es []raftpb.Entry, confState *raftpb.ConfState) (
	appliedi uint64, shouldStop bool) {

	for _, e := range es {
		switch e.Type {
		case raftpb.EntryNormal:
			n.applyEntry(e)

		case raftpb.EntryConfChange:
			n.applyConfChange(e, confState)

		default:
			glog.Fatalf("unexpected entry type")
		}
		appliedi = e.Index
	}
	return
}

func (n *Node) snapshot(snapi uint64, confs raftpb.ConfState) {
	d, err := n.stateMachine.Save()
	if err != nil {
		glog.Fatalf("error in seralizing the state machine: %v", err)
	}

	go func() {
		snap, err := n.raftStorage.CreateSnapshot(snapi, &confs, d)
		if err != nil {
			// the snapshot was done asynchronously with the progress of raft.
			// raft might have already got a newer snapshot.
			if err == etcdraft.ErrSnapOutOfDate {
				return
			}
			glog.Fatalf("unexpected create snapshot error %v", err)
		}

		if err := n.diskStorage.SaveSnap(snap); err != nil {
			glog.Fatalf("save snapshot error: %v", err)
		}
		glog.Infof("saved snapshot at index %d", snap.Metadata.Index)

		// keep some in memory log entries for slow followers.
		compacti := uint64(1)
		if snapi > numberOfCatchUpEntries {
			compacti = snapi - numberOfCatchUpEntries
		}
		fmt.Println("snap compat ", compacti, snapi)
		if err = n.raftStorage.Compact(compacti); err != nil {
			// the compaction was done asynchronously with the progress of raft.
			// raft log might already been compact.
			if err == etcdraft.ErrCompacted {
				return
			}

			glog.Fatalf("unexpected compaction error %v", err)
		}
		glog.Infof("compacted raft log at %d", compacti)
	}()
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

func (n *Node) Campaign(ctx context.Context) error {
	return n.node.Campaign(ctx)
}

func (n *Node) Step(ctx context.Context, msg raftpb.Message) error {
	return n.node.Step(ctx, msg)
}
