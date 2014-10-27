package gob

import (
	gogob "encoding/gob"
)

type GobError struct {
	Err string
}

func (e GobError) Error() string {
	return e.Err
}

func init() {
	gogob.Register(GobError{})
}
