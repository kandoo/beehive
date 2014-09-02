package nom

import (
	"bytes"
	"encoding/gob"
	"encoding/json"

	"github.com/soheilhy/beehive/bh"
)

// Node represents a forwarding element, such as switches and routers.
type Node struct {
	ID     NodeID
	Net    UID
	Ports  map[PortID]Port
	Driver bh.BeeId
}

// NodeID is the ID of a node. This must be unique among all nodes in the
// network.
type NodeID string

// UID returns the node's unique ID. This id is in the form of net_id$$node_id.
func (n Node) UID() UID {
	return UIDJoin(string(n.Net), string(n.ID))
}

// ParseNodeUID parses a UID of a node and returns the respective network and
// node IDs.
func ParseNodeUID(id UID) (NetworkID, NodeID) {
	s := UIDSplit(id)
	return NetworkID(s[0]), NodeID(s[1])
}

// GOBDecode decodes the node from b using GOB.
func (n *Node) GOBDecode(b []byte) error {
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	return dec.Decode(n)
}

// GOBEncode encodes the node into a byte array using GOB.
func (n *Node) GOBEncode() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(n)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// JSONDecode decodes the node from a byte array using JSON.
func (n *Node) JSONDecode(b []byte) error {
	return json.Unmarshal(b, n)
}

// JSONEncode encodes the node into a byte array using JSON.
func (n *Node) JSONEncode() ([]byte, error) {
	return json.Marshal(n)
}
