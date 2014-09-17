package bh

type RemoteCmd struct {
	Cmd   interface{}
	CmdTo BeeID
}

type LocalCmd struct {
	RemoteCmd
	ResCh chan CmdResult
}

func NewRemoteCmd(cmd interface{}, to BeeID) RemoteCmd {
	return RemoteCmd{
		Cmd:   cmd,
		CmdTo: to,
	}
}

func NewLocalCmd(cmd interface{}, to BeeID, ch chan CmdResult) LocalCmd {
	return LocalCmd{
		RemoteCmd: NewRemoteCmd(cmd, to),
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

type stopCmd struct{}

type startCmd struct{}

type findBeeCmd struct {
	BeeID BeeID
}

type createBeeCmd struct{}

type joinColonyCmd struct {
	Colony BeeColony
}

type startDetachedCmd struct {
	Handler DetachedHandler
}

type bufferTxCmd struct {
	Tx []Tx
}

type commitTxCmd struct {
	Seq TxSeq
}

type getTxInfoCmd struct{}

type getTx struct {
	From TxSeq
	To   TxSeq
}

type migrateBeeCmd struct {
	From BeeID
	To   HiveID
}

type replaceBeeCmd struct {
	OldBees     BeeColony
	NewBees     BeeColony
	State       *inMemoryState
	MappedCells MappedCells
}

type lockMappedCellsCmd struct {
	Colony      BeeColony
	MappedCells MappedCells
}

type getColonyCmd struct{}

type addSlaveCmd struct {
	BeeID
}

type delSlaveCmd struct {
	BeeID
}

type addMappedCell struct {
	Cells MappedCells
}

func CreateBee(h HiveID, app AppName) (BeeID, error) {
	prx := NewProxy(h)
	to := BeeID{
		HiveID:  h,
		AppName: app,
	}
	cmd := NewRemoteCmd(createBeeCmd{}, to)
	d, err := prx.SendCmd(&cmd)
	if err != nil {
		return BeeID{}, err
	}

	return d.(BeeID), nil
}
