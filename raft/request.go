package raft

import "encoding/gob"

// RequestID represents a unique request throughout the cluster.
type RequestID struct {
	Node uint64
	Seq  uint64
}

// Request represents a request for the store.
type Request struct {
	Data interface{}
}

// Response represents a response to a request.
type Response struct {
	ID   RequestID   // Response ID is always set to the ID of the request.
	Data interface{} // Data is set if there was no error.
	Err  error       // Error, if any.
}

func init() {
	gob.Register(RequestID{})
	gob.Register(Request{})
	gob.Register(Response{})
}
