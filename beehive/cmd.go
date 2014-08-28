package beehive

type routineCmd struct {
	cmdType routineCmdType
	cmdData interface{}
	resCh   chan asyncResult
}

type routineCmdType int

const (
	stopCmd        routineCmdType = iota
	startCmd                      = iota
	findRcvrCmd                   = iota
	createRcvrCmd                 = iota
	migrateRcvrCmd                = iota
	replaceRcvrCmd                = iota
)

type migrateRcvrCmdData struct {
	From RcvrId
	To   StageId
}

type replaceRcvrCmdData struct {
	OldRcvr RcvrId
	NewRcvr RcvrId
	State   *inMemoryState
	MapSet  MapSet
}
