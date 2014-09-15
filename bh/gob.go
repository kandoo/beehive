package bh

type GobError struct {
	Err string
}

func (e GobError) Error() string {
	return e.Err
}
