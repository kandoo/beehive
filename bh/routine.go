package bh

type msgAndHandler struct {
	msg     *msg
	handler Handler
}

// The base structure shared between mappers and receivers.
type asyncRoutine struct {
	dataCh chan msgAndHandler
	ctrlCh chan routineCmd
}

type asyncResult struct {
	data interface{}
	err  error
}

func (r asyncResult) get() (interface{}, error) {
	return r.data, r.err
}
