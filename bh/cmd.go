package bh

type RemoteCmd struct {
	CmdType CmdType
	CmdData interface{}
	CmdTo   BeeID
}

type LocalCmd struct {
	RemoteCmd
	ResCh chan CmdResult
}

func NewRemoteCmd(t CmdType, d interface{}, to BeeID) RemoteCmd {
	return RemoteCmd{
		CmdType: t,
		CmdData: d,
		CmdTo:   to,
	}
}

func NewLocalCmd(t CmdType, d interface{}, to BeeID,
	ch chan CmdResult) LocalCmd {

	return LocalCmd{
		RemoteCmd: NewRemoteCmd(t, d, to),
		ResCh:     ch,
	}
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
	bufferTxCmd                = iota
	commitTxCmd                = iota
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
