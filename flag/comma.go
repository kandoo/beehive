package flag

import "strings"

// CSV implements a simple comma seperated value for golang flag.
type CSV struct {
	S *[]string
}

func (v CSV) String() string {
	return strings.Join([]string(*v.S), ",")
}

func (v CSV) Get() interface{} {
	return []string(*v.S)
}

func (v CSV) Set(val string) error {
	*v.S = strings.Split(val, ",")
	return nil
}
