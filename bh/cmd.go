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
	stopCmd            CmdType = iota
	startCmd                   = iota
	findBeeCmd                 = iota
	createBeeCmd               = iota
	migrateBeeCmd              = iota
	replaceBeeCmd              = iota
	lockMappedCellsCmd         = iota
	startDetachedCmd           = iota
	addSlaveCmd                = iota
	delSlaveCmd                = iota
	listSlavesCmd              = iota
)

type migrateBeeCmdData struct {
	From BeeID
	To   HiveID
}

type replaceBeeCmdData struct {
	OldBees     BeeColony
	NewBees     BeeColony
	State       *inMemoryState
	MappedCells MappedCells
}

type lockMappedCellsData struct {
	Colony      BeeColony
	MappedCells MappedCells
}

type addSlaveCmdData struct {
	BeeID
}

type delSlaveCmdData struct {
	BeeID
}
