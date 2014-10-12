package gob

import (
	"bytes"
	stdgob "encoding/gob"
)

func Encode(i interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := stdgob.NewEncoder(&buf)
	if err := enc.Encode(i); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func Decode(i interface{}, b []byte) error {
	buf := bytes.NewBuffer(b)
	dec := stdgob.NewDecoder(buf)
	return dec.Decode(i)
}
