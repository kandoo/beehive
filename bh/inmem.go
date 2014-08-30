package bh

import "errors"

// A simple dictionary that uses in memory maps.
type inMemoryState struct {
	Name  string
	Dicts map[string]*inMemoryDictionary
}

func (s *inMemoryState) StartTx() error {
	return errors.New("In memory state does not support transactions.")
}

func (s *inMemoryState) CommitTx() error {
	return errors.New("In memory state does not support transactions.")
}

func (s *inMemoryState) AbortTx() error {
	return errors.New("In memory state does not support transactions.")
}

func (s *inMemoryState) Dict(name DictionaryName) Dictionary {
	d, ok := s.Dicts[string(name)]
	if !ok {
		d = &inMemoryDictionary{name, make(map[Key]Value)}
		s.Dicts[string(name)] = d
	}
	return d
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
