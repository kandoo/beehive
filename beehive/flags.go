package beehive

import "strings"

type commaSeparatedValue struct {
	s *[]string
}

func (v commaSeparatedValue) String() string {
	return strings.Join([]string(*v.s), ",")
}

func (v commaSeparatedValue) Get() interface{} {
	return []string(*v.s)
}

func (v commaSeparatedValue) Set(val string) error {
	*v.s = strings.Split(val, ",")
	return nil
}
