package raft

import "github.com/kandoo/beehive/Godeps/_workspace/src/github.com/coreos/etcd/raft/raftpb"

// LeaderChanged indicate that the leader of the raft quorom is changed.
type LeaderChanged struct {
	Old  uint64 // The old leader.
	New  uint64 // The new leader.
	Term uint64 // The raft term.
}

// StateMachine represents an application defined state.
type StateMachine interface {
	// Save saves the store into bytes.
	Save() ([]byte, error)
	// Recover recovers the store from bytes.
	Restore(b []byte) error
	// Apply applies a request and returns the response.
	Apply(req interface{}) (interface{}, error)
	// ApplyConfChange processes a configuration change.
	ApplyConfChange(cc raftpb.ConfChange, n NodeInfo) error
	// ProcessStatusChange is called whenever the leader of the quorum is changed.
	ProcessStatusChange(event interface{})
}
