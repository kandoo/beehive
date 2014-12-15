package raft

import (
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/kandoo/beehive/Godeps/_workspace/src/code.google.com/p/go.net/context"
	etcdraft "github.com/kandoo/beehive/Godeps/_workspace/src/github.com/coreos/etcd/raft"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/coreos/etcd/raft/raftpb"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/coreos/etcd/snap"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/coreos/etcd/wal"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/golang/glog"
	"github.com/kandoo/beehive/gen"
	bhgob "github.com/kandoo/beehive/gob"
)

// Most of this code is adapted from etcd/etcdserver/server.go.

var (
	ErrStopped = errors.New("node stopped")
)

type SendFunc func(m []raftpb.Message)

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
		glog.Fatalf("Error in encoding peer: %v", err)
	}
	return b
}

type Node struct {
	name string
	id   uint64
	node etcdraft.Node
	line line
	gen  gen.IDGenerator

	listener  StatusListener
	store     Store
	wal       *wal.WAL
	snap      *snap.Snapshotter
	snapCount uint64

	send SendFunc

	ticker <-chan time.Time
	done   chan struct{}
}

func init() {
	gob.Register(NodeInfo{})
	gob.Register(RequestID{})
	gob.Register(Request{})
	gob.Register(Response{})
}

func NewNode(name string, id uint64, peers []etcdraft.Peer, send SendFunc,
	listener StatusListener, datadir string, store Store, snapCount uint64,
	ticker <-chan time.Time, election, heartbeat int) *Node {

	glog.V(2).Infof("creating a new raft node %v (%v) with peers %v", id, name,
		peers)

	snapdir := path.Join(datadir, "snap")
	if err := os.MkdirAll(snapdir, 0700); err != nil {
		glog.Fatal("raft: cannot create snapshot directory")
	}

	var lastIndex uint64
	var n etcdraft.Node
	ss := snap.New(snapdir)
	var w *wal.WAL
	waldir := path.Join(datadir, "wal")
	if !wal.Exist(waldir) {
		// We are creating a new node.
		if id == 0 {
			glog.Fatal("raft: node id cannot be 0")
		}

		glog.V(2).Infof("no WAL found for %v (%v). starting new node", id, name)
		var err error
		w, err = wal.Create(waldir, []byte(strconv.FormatUint(id, 10)))
		if err != nil {
			glog.Fatal(err)
		}
		n = etcdraft.StartNode(id, peers, election, heartbeat)
	} else {
		var index uint64
		snapshot, err := ss.Load()
		if err != nil && err != snap.ErrNoSnapshot {
			glog.Fatal(err)
		}

		if snapshot != nil {
			glog.Infof("restarting from snapshot at index %d", snapshot.Index)
			store.Restore(snapshot.Data)
			index = snapshot.Index
		}

		if w, err = wal.OpenAtIndex(waldir, index); err != nil {
			glog.Fatal(err)
		}
		md, st, ents, err := w.ReadAll()
		if err != nil {
			glog.Fatal(err)
		}

		walid, err := strconv.ParseUint(string(md), 10, 64)
		if err != nil {
			glog.Fatal(err)
		}

		if walid != id {
			glog.Fatal("id in write-ahead-log is %v and different than %v", walid, id)
		}

		n = etcdraft.RestartNode(id, election, heartbeat, snapshot, st, ents)
		lastIndex = ents[len(ents)-1].Index
	}

	node := &Node{
		name:      name,
		id:        id,
		node:      n,
		gen:       gen.NewSeqIDGen(lastIndex + 2*snapCount), // To avoid collisions.
		listener:  listener,
		store:     store,
		wal:       w,
		snap:      ss,
		snapCount: snapCount,
		send:      send,
		ticker:    ticker,
		done:      make(chan struct{}),
	}
	node.line.init()
	go node.Start()
	return node
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
	glog.V(2).Infof("%v applies normal entry %v: %#v", n, e.Index, e)

	if len(e.Data) == 0 {
		glog.V(2).Infof("raft entry %v has no data", e.Index)
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
	res.Data, res.Err = n.store.Apply(req.Data)
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

func (n *Node) validConfChange(cc raftpb.ConfChange, nodes []uint64) error {
	if cc.NodeID == etcdraft.None {
		return errors.New("node id is nil")
	}

	switch cc.Type {
	case raftpb.ConfChangeAddNode:
		if containsNode(nodes, cc.NodeID) {
			return fmt.Errorf("%v is duplicate", cc.NodeID)
		}
	case raftpb.ConfChangeRemoveNode:
		if !containsNode(nodes, cc.NodeID) {
			return fmt.Errorf("No such node", cc.NodeID)
		}
	default:
		glog.Fatalf("invalid ConfChange type %v", cc.Type)
	}
	return nil
}

func (n *Node) applyConfChange(e raftpb.Entry, nodes []uint64) {
	var cc raftpb.ConfChange
	if err := cc.Unmarshal(e.Data); err != nil {
		glog.Fatalf("raftserver: cannot decode confchange (%v)", err)
	}

	glog.V(2).Infof("%v applies conf change %v: %#v", n, e.Index, cc)

	if err := n.validConfChange(cc, nodes); err != nil {
		glog.V(2).Infof("%v received an invalid conf change for node %v: %v",
			n, cc.NodeID, err)
		cc.NodeID = etcdraft.None
		n.node.ApplyConfChange(cc)
		return
	}

	n.node.ApplyConfChange(cc)
	if len(cc.Context) == 0 {
		n.store.ApplyConfChange(cc, NodeInfo{})
		return
	}

	var info NodeInfo
	if err := bhgob.Decode(&info, cc.Context); err == nil {
		if info.ID != cc.NodeID {
			glog.Fatalf("invalid config change: %v != %v", info.ID, cc.NodeID)
		}
		n.store.ApplyConfChange(cc, info)
		return
	}

	var req Request
	if err := req.Decode(cc.Context); err != nil {
		// It should be either a node info or a request.
		glog.Fatalf("raftserver: cannot decode context (%v)", err)
	}
	res := Response{
		ID: req.ID,
	}
	res.Err = n.store.ApplyConfChange(cc, req.Data.(NodeInfo))
	n.line.call(res)
}

func (n *Node) Start() {
	glog.V(2).Infof("%v started", n)
	var snapi, appliedi uint64
	var nodes []uint64
	var prevss *etcdraft.SoftState
	for {
		select {
		case <-n.ticker:
			n.node.Tick()
		case rd := <-n.node.Ready():
			if rd.SoftState != nil {
				if prevss != nil && prevss.Lead != rd.SoftState.Lead {
					n.listener.ProcessStatusChange(LeaderChanged{
						Old: prevss.Lead,
						New: rd.SoftState.Lead,
					})
				}
				nodes = rd.SoftState.Nodes
				prevss = rd.SoftState
			}

			n.wal.Save(rd.HardState, rd.Entries)
			n.snap.SaveSnap(rd.Snapshot)
			n.send(rd.Messages)

			if len(rd.CommittedEntries) > 0 {
				glog.V(2).Infof("%v receives raft update", n)
			}

			for _, e := range rd.CommittedEntries {
				switch e.Type {
				case raftpb.EntryNormal:
					n.applyEntry(e)

				case raftpb.EntryConfChange:
					n.applyConfChange(e, nodes)

				default:
					panic("unexpected entry type")
				}

				appliedi = e.Index
			}

			if rd.Snapshot.Index > snapi {
				snapi = rd.Snapshot.Index
			}

			// Recover from snapshot if it is more recent than the currently applied.
			if rd.Snapshot.Index > appliedi {
				if err := n.store.Restore(rd.Snapshot.Data); err != nil {
					glog.Fatalf("TODO: %v", err)
				}
				appliedi = rd.Snapshot.Index
			}

			if appliedi-snapi > n.snapCount {
				n.snapshot(appliedi, nodes)
				snapi = appliedi
			}
		case <-n.done:
			return
		}
	}
}

func (n *Node) snapshot(snapi uint64, snapnodes []uint64) {
	d, err := n.store.Save()
	if err != nil {
		glog.Fatalf("TODO: %v", err)
	}
	n.node.Compact(snapi, snapnodes, d)
	n.wal.Cut()
}

func (n *Node) String() string {
	return fmt.Sprintf("node %v (%v)", n.id, n.name)
}

func (n *Node) Stop() {
	n.node.Stop()
	close(n.done)
}

func (n *Node) Campaign(ctx context.Context) error {
	return n.node.Campaign(ctx)
}

func (n *Node) Step(ctx context.Context, msg raftpb.Message) error {
	return n.node.Step(ctx, msg)
}
