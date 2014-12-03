package raft

import (
	"encoding/gob"

	bhgob "github.com/kandoo/beehive/gob"
)

// RequestID represents a unique request throughout the cluster.
type RequestID struct {
	NodeID uint64
	Seq    uint64
}

// Request represents a request for the store.
type Request struct {
	ID   RequestID
	Data interface{}
}

func (r *Request) Decode(b []byte) error {
	return bhgob.Decode(r, b)
}

func (r *Request) Encode() ([]byte, error) {
	return bhgob.Encode(r)
}

// Response represents a response to a request.
type Response struct {
	ID   RequestID   // Response ID is always set to the ID of the request.
	Data interface{} // Data is set if there was no error.
	Err  error       // Error, if any.
}

func init() {
	gob.Register(Request{})
	gob.Register(Response{})
}
