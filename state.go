package beehive

import "github.com/soheilhy/beehive/state"

// State is the storage for a collection of dictionaries.
type State interface {
	// Returns a dictionary for this state. Creates one if it does not exist.
	Dict(name string) state.Dict
}
