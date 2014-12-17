package raft

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/coreos/etcd/raft/raftpb"
)

var (
	ErrEmptyMessage = errors.New("raft: empty message")
)

// Encoder encodes size delimited raft messages in a bytes.Buffer.
type Encoder struct {
	w   io.Writer
	off int
}

// NewEncoder creates an Encoder that encodes messages in w.
func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{w: w}
}

// Encode
func (e *Encoder) Encode(m raftpb.Message) error {
	var buf [4]byte
	b := buf[:4]
	s := m.Size()
	if s == 0 {
		return ErrEmptyMessage
	}
	binary.BigEndian.PutUint32(b, uint32(s))
	e.w.Write(b)
	e.off += 4
	b, err := m.Marshal()
	if err != nil {
		return err
	}
	e.w.Write(b)
	e.off += s
	return nil
}
