package raft

import (
	"bytes"
	"io"
	"reflect"
	"testing"

	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/coreos/etcd/raft/raftpb"
)

func TestEncoding(t *testing.T) {
	msgs := []raftpb.Message{
		{To: 1, From: 1},
		{To: 2, From: 2, Entries: []raftpb.Entry{{Term: 2, Data: []byte{2}}}},
		{To: 3, From: 3},
		{To: 4, From: 4, Entries: []raftpb.Entry{{Term: 2, Data: []byte{4, 4}}}},
	}
	s := 0
	var buf bytes.Buffer
	enc := NewEncoder(&buf)
	for _, m := range msgs {
		s += m.Size()
		if err := enc.Encode(m); err != nil {
			t.Fatalf("cannot encode message: %v", err)
		}
	}
	b := buf.Bytes()
	if len(b) != s+len(msgs)*4 {
		t.Errorf("invalid number of bytes: actual=%d want=%d", len(b),
			s+len(msgs)*4)
	}
	dec := NewDecoder(&buf)
	dmsgs := make([]raftpb.Message, len(msgs))
	for i := range dmsgs {
		if err := dec.Decode(&dmsgs[i]); err != nil {
			t.Fatalf("cannot decode: %v", err)
			continue
		}
		if !reflect.DeepEqual(msgs[i], dmsgs[i]) {
			t.Errorf("invalid decoded message: actual=%#v want=%#v", msgs[i],
				dmsgs[i])
		}
	}
}

func ExampleEncoder() {
	msgs := []raftpb.Message{ /*...*/}
	var buf bytes.Buffer
	enc := NewEncoder(&buf)
	for _, m := range msgs {
		if err := enc.Encode(m); err != nil {
			panic(err)
		}
	}
}

func ExampleDecoder() {
	var msgs []raftpb.Message
	var buf bytes.Buffer
	dec := NewDecoder(&buf)
	for {
		var msg raftpb.Message
		err := dec.Decode(&msg)
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		msgs = append(msgs, msg)
	}
}
