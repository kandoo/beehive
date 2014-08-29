package bh

import "errors"

// State is the storage for a collection of dictionaries.
type State interface {
	Dict(name DictionaryName) Dictionary
}

type IterFn func(k Key, v Value)

// Simply a key-value store.
type Dictionary interface {
	Get(k Key) (Value, error)
	Put(k Key, v Value) error
	Del(k Key) error
	ForEach(f IterFn)
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

func newState(name string) State {
	return &inMemoryState{name, make(map[string]*inMemoryDictionary)}
}

type inMemoryState struct {
	Name  string
	Dicts map[string]*inMemoryDictionary
}

type inMemoryDictionary struct {
	Name DictionaryName
	Dict map[Key]Value
}

func (d *inMemoryDictionary) Get(k Key) (Value, error) {
	v, ok := d.Dict[k]
	if !ok {
		return v, errors.New("Key does not exist.")
	}
	return v, nil
}

func (d *inMemoryDictionary) Put(k Key, v Value) error {
	d.Dict[k] = v
	return nil
}

func (d *inMemoryDictionary) Del(k Key) error {
	delete(d.Dict, k)
	return nil
}

func (d *inMemoryDictionary) ForEach(f IterFn) {
	for k, v := range d.Dict {
		f(k, v)
	}
}

func (s *inMemoryState) Dict(name DictionaryName) Dictionary {
	d, ok := s.Dicts[string(name)]
	if !ok {
		d = &inMemoryDictionary{name, make(map[Key]Value)}
		s.Dicts[string(name)] = d
	}
	return d
}
