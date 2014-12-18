package raft

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/coreos/etcd/raft/raftpb"
)

var (
	ErrNoEnoughData = errors.New("raft: not enough data")
)

// Decoder decodes size delimited raft messages in a bytes.Buffer.
type Decoder struct {
	r io.Reader
}

// NewDecoder creates a Decoder that decodes messages from r.
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{r: r}
}

// Decode decodes the next raft message. If there is no data to read it returns
// io.EOF.
func (d *Decoder) Decode(m *raftpb.Message) error {
	var buf [4]byte
	b := buf[:4]
	n, err := d.r.Read(b)
	if err != nil {
		return err
	}
	if n != 4 {
		return ErrNoEnoughData
	}
	s := int(binary.BigEndian.Uint32(b))
	b = make([]byte, s)
	n, err = io.ReadFull(d.r, b)
	if err != nil {
		return err
	}
	if n != s {
		return ErrNoEnoughData
	}
	m.Unmarshal(b)
	return nil
}
