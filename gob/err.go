package gob

import (
	gogob "encoding/gob"
)

type Error string

func (e Error) Error() string {
	return string(e)
}

func init() {
	gogob.Register(Error(""))
}
