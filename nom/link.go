package nom

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
)

// Link represents an outgoing link from a port.
type Link struct {
	ID   LinkID // Link's ID.
	From UID    // From is the link's port.
	To   []UID  // To stores the port(s) connected to From using this link.
}

// LinkID is a link's ID which is unique among the outgoing links of a port.
type LinkID string

// UID returns the UID of the link in the form of
// net_id$$node_id$$port_id$$link_id.
func (l Link) UID() UID {
	return UIDJoin(string(l.From), string(l.ID))
}

// ParseLinkUID parses a link UID into the respetive network, node, port, and
// link ids.
func ParseLinkUID(id UID) (NetworkID, NodeID, PortID, LinkID) {
	s := UIDSplit(id)
	return NetworkID(s[0]), NodeID(s[1]), PortID(s[2]), LinkID(s[3])
}

// GOBDecode decodes the link from b using GOB.
func (l *Link) GOBDecode(b []byte) error {
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	return dec.Decode(l)
}

// GOBEncode encodes the node into a byte array using GOB.
func (l *Link) GOBEncode() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(l)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// JSONDecode decodes the node from a byte array using JSON.
func (l *Link) JSONDecode(b []byte) error {
	return json.Unmarshal(b, l)
}

// JSONEncode encodes the node into a byte array using JSON.
func (l *Link) JSONEncode() ([]byte, error) {
	return json.Marshal(l)
}
