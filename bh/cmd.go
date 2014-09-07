package bh

type routineCmd struct {
	cmdType routineCmdType
	cmdData interface{}
	resCh   chan asyncResult
}

type routineCmdType int

const (
	stopCmd          routineCmdType = iota
	startCmd                        = iota
	findBeeCmd                      = iota
	createBeeCmd                    = iota
	migrateBeeCmd                   = iota
	replaceBeeCmd                   = iota
	lockMapSetCmd                   = iota
	startDetachedCmd                = iota
)

type migrateBeeCmdData struct {
	From BeeId
	To   HiveId
}

type replaceBeeCmdData struct {
	OldBee BeeId
	NewBee BeeId
	State  *inMemoryState
	MapSet MapSet
}

type lockMapSetData struct {
	BeeId  BeeId
	MapSet MapSet
}
