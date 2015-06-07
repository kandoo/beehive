package raft

// LeaderChanged indicate that the leader of the raft quorom is changed.
type LeaderChanged struct {
	Old  uint64 // The old leader.
	New  uint64 // The new leader.
	Term uint64 // The raft term.
}

// StatusListener processes status change events.
type StatusListener interface {
	ProcessStatusChange(event interface{})
}
