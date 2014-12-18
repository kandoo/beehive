package gob

import (
	gogob "encoding/gob"
	"fmt"
)

type Error string

func (e Error) Error() string {
	return string(e)
}

func Errorf(format string, args ...interface{}) Error {
	return Error(fmt.Sprintf(format, args))
}

func init() {
	gogob.Register(Error(""))
}
