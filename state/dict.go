package state

// IterFn is the function used to iterate the entries of a dictionary. If
// it returns false the foreach loop will stop.
type IterFn func(key string, val interface{}) (next bool)

// Dict is a simple key-value store.
type Dict interface {
	// Name returns the name the dictionary.
	Name() string

	// Get the value associated with k.
	Get(key string) (val interface{}, err error)
	// Associate value with the key.
	Put(key string, val interface{}) error
	// Del deletes key from dictionary.
	Del(key string) error
	// ForEach iterates over all entries in the dictionary, and invokes f for
	// each entry.
	ForEach(f IterFn)
}
