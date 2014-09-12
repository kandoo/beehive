package bh

type RemoteCmd struct {
	CmdType CmdType
	CmdData interface{}
	CmdTo   BeeId
}

type LocalCmd struct {
	CmdType CmdType
	CmdData interface{}
	ResCh   chan CmdResult
}

type CmdResult struct {
	Data interface{}
	Err  error
}

func (r CmdResult) get() (interface{}, error) {
	return r.Data, r.Err
}

type CmdType int

const (
	stopCmd          CmdType = iota
	startCmd                 = iota
	findBeeCmd               = iota
	createBeeCmd             = iota
	migrateBeeCmd            = iota
	replaceBeeCmd            = iota
	lockMapSetCmd            = iota
	startDetachedCmd         = iota
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
