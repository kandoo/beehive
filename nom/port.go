package nom

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
)

// Port is either a physical or a virtual port of a node.
type Port struct {
	ID    PortID
	Node  UID
	Links []UID
}

// PortID is the ID of a port and is unique among the ports of a node.
type PortID string

// UID returns the unique ID of the port in the form of
// net_id$$node_id$$port_id.
func (p Port) UID() UID {
	return UIDJoin(string(p.Node), string(p.ID))
}

// ParsePortUID parses a UID of a port and returns the respective network, node,
// and port IDs.
func ParsePortUID(id UID) (NetworkID, NodeID, PortID) {
	s := UIDSplit(id)
	return NetworkID(s[0]), NodeID(s[1]), PortID(s[2])
}

// AddLink adds an outgoing link to the port. Return an error if the link does
// not belong to this port.
func (p *Port) AddLink(l Link) error {
	if l.From != p.UID() {
		return fmt.Errorf("Link is outgoing from %s not from %s", l.From, p.UID())
	}

	p.Links = append(p.Links, l.UID())
	return nil
}

// GOBDecode decodes the port from b using GOB.
func (p *Port) GOBDecode(b []byte) error {
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	return dec.Decode(p)
}

// GOBEncode encodes the port into a byte array using GOB.
func (p *Port) GOBEncode() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(p)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// JSONDecode decodes the port from a byte array using JSON.
func (p *Port) JSONDecode(b []byte) error {
	return json.Unmarshal(b, p)
}

// JSONEncode encodes the port into a byte array using JSON.
func (p *Port) JSONEncode() ([]byte, error) {
	return json.Marshal(p)
}
