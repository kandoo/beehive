package raft

import (
	"bytes"
	"fmt"

	etcdraft "github.com/kandoo/beehive/Godeps/_workspace/src/github.com/coreos/etcd/raft"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/coreos/etcd/raft/raftpb"
)

func formatData(d []byte) string {
	if len(d) < 4 {
		return fmt.Sprintf("%q", d[:len(d)])
	}
	return fmt.Sprintf("%q..%q", d[:2], d[len(d)-2:])
}

func formatEntry(e raftpb.Entry) string {
	return etcdraft.DescribeEntry(e, formatData)
}

func formatEntries(es []raftpb.Entry) string {
	var w bytes.Buffer
	w.WriteRune('[')
	for i, e := range es {
		if i != 0 {
			w.WriteRune(',')
		}
		fmt.Fprint(&w, formatEntry(e))
	}
	w.WriteRune(']')
	return w.String()
}

func formatConfChange(cc raftpb.ConfChange) string {
	return fmt.Sprintf("%s@%s %s", cc.NodeID, cc.ID, cc.Type)
}
