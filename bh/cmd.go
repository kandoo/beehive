package bh

type RemoteCmd struct {
	CmdType CmdType
	CmdData interface{}
	CmdTo   BeeID
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
	From BeeID
	To   HiveID
}

type replaceBeeCmdData struct {
	OldBee BeeID
	NewBee BeeID
	State  *inMemoryState
	MapSet MapSet
}

type lockMapSetData struct {
	BeeID  BeeID
	MapSet MapSet
}
