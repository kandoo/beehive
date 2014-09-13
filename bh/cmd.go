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
	lockMappedCellsCmd            = iota
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
	MappedCells MappedCells
}

type lockMappedCellsData struct {
	BeeID  BeeID
	MappedCells MappedCells
}
