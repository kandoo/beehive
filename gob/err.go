package gob

import (
	gogob "encoding/gob"
	"fmt"
)

type Error string

func (e Error) Error() string {
	return string(e)
}

func (e Error) IsNil() bool {
	return e == ""
}

func NewError(err error) Error {
	if err == nil {
		return ""
	}
	return Error(err.Error())
}

func Errorf(format string, args ...interface{}) Error {
	return Error(fmt.Sprintf(format, args...))
}

func init() {
	gogob.Register(Error(""))
}
