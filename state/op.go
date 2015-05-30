package state

// OpType is the type of an operation in a transaction.
type OpType int

// Valid values for OpType.
const (
	Unknown OpType = iota
	Put
	Del
)

// Op is a state operation in a transaction.
type Op struct {
	T OpType
	D string      // Dictionary.
	K string      // Key.
	V interface{} // Value.
}
