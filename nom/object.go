package nom

// Object is the interface of all structs in the network object model.
type Object interface {
	// GOBDecode decodes the object from a byte array using the GOB encoding.
	GOBDecode(b []byte) error
	// GOBEncode encodes the object into a byte array using the GOB encoding.
	GOBEncode() ([]byte, error)
	// JSONDecode decodes the object from a byte array using the JSON encoding.
	JSONDecode(b []byte) error
	// JSONEncode encodes the object into a byte array using the JSON encoding.
	JSONEncode() ([]byte, error)
	// UID returns a unique ID of this object. This ID is unique in the network
	// among all other objects.
	UID() UID
}
