package beehive

import (
	"encoding/gob"

	"github.com/kandoo/beehive/raft"
)

type cmdAddFollower struct {
	Hive uint64
	Bee  uint64
}
type cmdAddHive struct{ Info raft.NodeInfo }
type cmdCampaign struct{}
type cmdCreateBee struct{}
type cmdFindBee struct{ ID uint64 }
type cmdHandoff struct{ To uint64 }
type cmdJoinColony struct{ Colony Colony }
type cmdLiveHives struct{}
type cmdMigrate struct {
	Bee uint64
	To  uint64
}
type cmdNewHiveID struct{ Addr string }
type cmdPing struct{}
type cmdReloadBee struct{ ID uint64 }
type cmdStart struct{}
type cmdStartDetached struct{ Handler DetachedHandler }
type cmdStop struct{}
type cmdSync struct{}

func init() {
	gob.Register(cmdAddFollower{})
	gob.Register(cmdAddHive{})
	gob.Register(cmdCampaign{})
	gob.Register(cmdCreateBee{})
	gob.Register(cmdFindBee{})
	gob.Register(cmdHandoff{})
	gob.Register(cmdJoinColony{})
	gob.Register(cmdLiveHives{})
	gob.Register(cmdMigrate{})
	gob.Register(cmdAddHive{})
	gob.Register(cmdFindBee{})
	gob.Register(cmdCreateBee{})
	gob.Register(cmdNewHiveID{})
	gob.Register(cmdPing{})
	gob.Register(cmdReloadBee{})
	gob.Register(cmdStart{})
	gob.Register(cmdStartDetached{})
	gob.Register(cmdStop{})
	gob.Register(cmdSync{})
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
