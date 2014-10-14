package state

import (
	"bytes"
	"encoding/gob"
)

type IterFn func(k string, v []byte)

// Dict is a simple key-value store.
type Dict interface {
	Name() string

	Get(k string) ([]byte, error)
	Put(k string, v []byte) error
	Del(k string) error
	ForEach(f IterFn)

	// GetGob retrieves the value stored for k in d, and decodes it into v using
	// gob. Returns error when there is no value or when it cannot decode it.
	GetGob(k string, v interface{}) error
	// PutGob encodes v using gob and store it for key k in d.
	PutGob(k string, v interface{}) error
}

func GetGob(d Dict, k string, v interface{}) error {
	dv, err := d.Get(k)
	if err != nil {
		return err
	}
	buf := bytes.NewBuffer(dv)
	dec := gob.NewDecoder(buf)
	return dec.Decode(v)
}

func PutGob(d Dict, k string, v interface{}) error {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(v); err != nil {
		return err
	}
	d.Put(k, buf.Bytes())
	return nil
}
