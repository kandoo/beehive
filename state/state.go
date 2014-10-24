package state

// State is a collection of dictionaries.
type State interface {
	// Returns a dictionary for this state. Creates one if it does not exist.
	Dict(name string) Dict
	// Save save the state into bytes.
	Save() ([]byte, error)
	// Restore restores the state from b.
	Restore(b []byte) error
}
