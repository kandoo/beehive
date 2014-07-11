package actor

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
	migrateRcvrCmd                = iota
	newRcvrCmd                    = iota
)

type migrateRcvrCmdData struct {
	From RcvrId
	To   StageId
}
