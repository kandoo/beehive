package raft

import "github.com/soheilhy/beehive/gob"

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
	return gob.Decode(r, b)
}

func (r *Request) Encode() ([]byte, error) {
	return gob.Encode(r)
}

// Response represents a response to a request.
type Response Request

func (r *Response) Decode(b []byte) error {
	return gob.Decode(r, b)
}

func (r *Response) Encode() ([]byte, error) {
	return gob.Encode(r)
}
