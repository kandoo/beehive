package beehive

import "github.com/soheilhy/beehive/raft"

type cmdStop struct{}

type cmdStart struct{}

type cmdPingHive struct{}

type cmdLiveHives struct{}

type cmdNewHiveID struct {
	Addr string
}

type cmdSync struct{}

type cmdAddHive struct {
	Info raft.NodeInfo
}

type cmdFindBee struct {
	ID uint64
}

type cmdCreateBee struct{}

type cmdReloadBee struct {
	ID uint64
}

type cmdJoinColony struct {
	Colony Colony
}

type cmdStartDetached struct {
	Handler DetachedHandler
}

// FIXME REFACTOR
//type bufferTxCmd struct {
//Tx Tx
//}

//type commitTxCmd struct {
//Seq TxSeq
//}

//type getTxInfoCmd struct{}

//type getTx struct {
//From TxSeq
//To   TxSeq
//}

//type migrateBeeCmd struct {
//From BeeID
//To   HiveID
//}

//type replaceBeeCmd struct {
//OldBees     BeeColony
//NewBees     BeeColony
//State       *inMemoryState
//TxBuf       []Tx
//MappedCells MappedCells
//}

//type lockMappedCellsCmd struct {
//Colony      BeeColony
//MappedCells MappedCells
//}

//type getColonyCmd struct{}

//type addSlaveCmd struct {
//BeeID
//}

//type delSlaveCmd struct {
//BeeID
//}

//type addMappedCells struct {
//Cells MappedCells
//}

//func (h *hive) createBee(hive HiveID, app AppName) (BeeID,
//error) {

//prx, err := h.newProxy(hive)
//if err != nil {
//return BeeID{}, err
//}

//to := BeeID{
//HiveID:  hive,
//AppName: app,
//}
//cmd := NewRemoteCmd(createBeeCmd{}, to)
//d, err := prx.sendCmd(&cmd)
//if err != nil {
//return BeeID{}, err
//}

//return d.(BeeID), nil
//}
