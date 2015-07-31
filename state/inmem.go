package state

import (
	"bytes"
	"encoding/gob"
)

// InMem is a simple dictionary that uses in memory maps.
type InMem struct {
	InMemDicts map[string]*inMemDict
}

// NewInMem creates a new InMem state.
func NewInMem() *InMem {
	return &InMem{
		InMemDicts: make(map[string]*inMemDict),
	}
}

func (s *InMem) Save() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(s); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (s *InMem) Restore(b []byte) error {
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	return dec.Decode(s)
}

func (s *InMem) Dict(name string) Dict {
	return s.inMemDict(name)
}

func (s *InMem) Dicts() []Dict {
	var dicts []Dict
	for _, d := range s.InMemDicts {
		dicts = append(dicts, d)
	}
	return dicts
}

func (s *InMem) inMemDict(name string) *inMemDict {
	d, ok := s.InMemDicts[name]
	if !ok {
		d = &inMemDict{
			DictName: name,
			Dict:     make(map[string]interface{}),
		}
		s.InMemDicts[name] = d
	}
	return d
}

type inMemDict struct {
	DictName string
	Dict     map[string]interface{}
}

func (d inMemDict) Name() string {
	return d.DictName
}

func (d *inMemDict) Get(k string) (interface{}, error) {
	v, ok := d.Dict[k]
	if !ok {
		return v, ErrNoSuchKey
	}
	return v, nil
}

func (d *inMemDict) Put(k string, v interface{}) error {
	d.Dict[k] = v
	return nil
}

func (d *inMemDict) Del(k string) error {
	if _, ok := d.Dict[k]; !ok {
		return ErrNoSuchKey
	}

	delete(d.Dict, k)
	return nil
}

func (d *inMemDict) ForEach(f IterFn) {
	for k, v := range d.Dict {
		if !f(k, v) {
			return
		}
	}
}
