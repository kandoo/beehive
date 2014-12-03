package state

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
)

var (
	ErrTxOpen = errors.New("State has an open transaction")
)

// InMem is a simple dictionary that uses in memory maps.
type InMem struct {
	Dicts map[string]*inMemDict
}

// NewInMem creates a new InMem state.
func NewInMem() *InMem {
	return &InMem{
		Dicts: make(map[string]*inMemDict),
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

func (s *InMem) inMemDict(name string) *inMemDict {
	d, ok := s.Dicts[name]
	if !ok {
		d = &inMemDict{name, make(map[string][]byte)}
		s.Dicts[name] = d
	}
	return d
}

type inMemDict struct {
	DictName string
	Dict     map[string][]byte
}

func (d inMemDict) Name() string {
	return d.DictName
}

func (d *inMemDict) Get(k string) ([]byte, error) {
	v, ok := d.Dict[k]
	if !ok {
		return v, fmt.Errorf("%v does not exist", k)
	}
	return v, nil
}

func (d *inMemDict) Put(k string, v []byte) error {
	d.Dict[k] = v
	return nil
}

func (d *inMemDict) Del(k string) error {
	delete(d.Dict, k)
	return nil
}

func (d *inMemDict) ForEach(f IterFn) {
	for k, v := range d.Dict {
		f(k, v)
	}
}

func (d *inMemDict) GetGob(k string, v interface{}) error {
	return GetGob(d, k, v)
}

func (d *inMemDict) PutGob(k string, v interface{}) error {
	return PutGob(d, k, v)
}
