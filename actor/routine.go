package actor

type routineCommand int

const (
	stopActor  routineCommand = iota
	startActor                = iota
)

type msgAndHandler struct {
	msg     Msg
	handler Handler
}

// The base structure shared between mappers and receivers.
type asyncRoutine struct {
	dataCh chan msgAndHandler
	ctrlCh chan routineCommand
	waitCh chan interface{}
}
