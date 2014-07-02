package actor

// State is the storage for a collection of dictionaries.
type State interface {
	Dict(name DictionaryName) Dictionary
}

// Simply a key-value store.
type Dictionary interface {
	Name() DictionaryName
	Get(key Key) (Value, bool)
	Set(key Key, val Value)
}

// DictionaryName is the key to lookup dictionaries in the state.
type DictionaryName string

// Key is to lookup values in Dicitonaries and is simply a string.
type Key string

// Dictionary values can be anything.
type Value interface{}

type DictionaryKey struct {
	Dict DictionaryName
	Key  Key
}

func (dk *DictionaryKey) String() string {
	// TODO(soheil): This will change when we implement it using a Trie instead of
	// a map.
	return string(dk.Dict) + "/" + string(dk.Key)
}

// This is the list of dictionary keys returned by the map functions.
type MapSet []DictionaryKey

func (ms MapSet) Len() int      { return len(ms) }
func (ms MapSet) Swap(i, j int) { ms[i], ms[j] = ms[j], ms[i] }
func (ms MapSet) Less(i, j int) bool {
	return ms[i].Dict < ms[j].Dict ||
		(ms[i].Dict == ms[j].Dict && ms[i].Key < ms[j].Key)
}
