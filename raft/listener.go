package raft

// LeaderChanged indicate that the leader of the raft quorom is changed.
type LeaderChanged struct {
	Old uint64 // The old leader.
	New uint64 // The new leader.
}

// StatusListener processes status change events.
type StatusListener interface {
	ProcessStatusChange(event interface{})
}
