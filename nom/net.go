package nom

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
)

// Network represents a virtual or physical network in NOM.
type Network struct {
	ID   NetworkID // The id of the network.
	Desc string    // A human-readable description of the network.
}

// NetworkID is the ID of the network.
type NetworkID string

// UID returns the UID of the network.
func (n Network) UID() UID {
	return UID(n.ID)
}

// ParseNetworkUID parses a network UID into the network ID. Note that for
// network these IDs of the same.
func ParseNetworkUID(id UID) NetworkID {
	return NetworkID(id)
}

// GOBDecode decodes the network from b using GOB.
func (n *Network) GOBDecode(b []byte) error {
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	return dec.Decode(n)
}

// GOBEncode encodes the network into a byte array using GOB.
func (n *Network) GOBEncode() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(n)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// JSONDecode decodes the network from a byte array using JSON.
func (n *Network) JSONDecode(b []byte) error {
	return json.Unmarshal(b, n)
}

// JSONEncode encodes the network into a byte array using JSON.
func (n *Network) JSONEncode() ([]byte, error) {
	return json.Marshal(n)
}
