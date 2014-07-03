package actor

type routineCmd struct {
	cmdType routineCmdType
	cmdData interface{}
	resCh   chan interface{}
}

type routineCmdType int

const (
	stopRoutine  routineCmdType = iota
	startRoutine                = iota
	findRcvr                    = iota
)

type msgAndHandler struct {
	msg     Msg
	handler Handler
}

// The base structure shared between mappers and receivers.
type asyncRoutine struct {
	dataCh chan msgAndHandler
	ctrlCh chan routineCmd
	waitCh chan interface{}
}
